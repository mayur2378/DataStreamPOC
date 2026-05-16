import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { VpcStack } from './vpc-stack';
import { RdsStack } from './rds-stack';
import { MskStack } from './msk-stack';

interface ConnectorsStackProps extends cdk.StackProps {
  vpcStack: VpcStack;
  rdsStack: RdsStack;
  mskStack: MskStack;
}

export class ConnectorsStack extends cdk.Stack {
  public readonly pluginBucket: s3.Bucket;
  public readonly executionRole: iam.Role;

  constructor(scope: Construct, id: string, props: ConnectorsStackProps) {
    super(scope, id, props);

    const { vpcStack, rdsStack, mskStack } = props;

    // S3 bucket for Kafka Connect plugin ZIPs.
    // setup-connectors.sh uploads the JARs here before creating MSK Connect resources.
    this.pluginBucket = new s3.Bucket(this, 'PluginBucket', {
      bucketName: `datastream-poc-plugins-${this.account}-${this.region}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const connectLogGroup = new logs.LogGroup(this, 'ConnectLogs', {
      logGroupName: '/datastream-poc/msk-connect',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // IAM execution role that MSK Connect worker tasks assume.
    this.executionRole = new iam.Role(this, 'MskConnectRole', {
      roleName: 'datastream-poc-msk-connect-role',
      assumedBy: new iam.ServicePrincipal('kafkaconnect.amazonaws.com'),
      description: 'MSK Connect execution role for POC',
    });

    // Read connector plugins from S3
    this.pluginBucket.grantRead(this.executionRole);

    // Read DB credentials from Secrets Manager
    rdsStack.secret.grantRead(this.executionRole);

    // Write logs to CloudWatch
    connectLogGroup.grantWrite(this.executionRole);

    // MSK cluster access (describe + read/write data)
    this.executionRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'kafka-cluster:Connect',
        'kafka-cluster:DescribeCluster',
        'kafka-cluster:AlterCluster',
        'kafka-cluster:ReadData',
        'kafka-cluster:WriteData',
        'kafka-cluster:CreateTopic',
        'kafka-cluster:DescribeTopic',
        'kafka-cluster:AlterTopic',
        'kafka-cluster:DescribeGroup',
        'kafka-cluster:AlterGroup',
        'kafka-cluster:DescribeTopicDynamicConfiguration',
        'kafka-cluster:AlterTopicDynamicConfiguration',
        'kafka-cluster:DescribeClusterDynamicConfiguration',
        'kafka-cluster:AlterClusterDynamicConfiguration',
        'kafka-cluster:DescribeTransactionalId',
        'kafka-cluster:AlterTransactionalId',
      ],
      resources: [
        mskStack.clusterArn,
        `${mskStack.clusterArn}/*`,
        `arn:aws:kafka:${this.region}:${this.account}:topic/datastream-poc/*`,
        `arn:aws:kafka:${this.region}:${this.account}:group/datastream-poc/*`,
        `arn:aws:kafka:${this.region}:${this.account}:transactional-id/datastream-poc/*`,
      ],
    }));

    // MSK Connect needs to describe the cluster to find bootstrap brokers
    this.executionRole.addToPolicy(new iam.PolicyStatement({
      actions: ['kafka:GetBootstrapBrokers', 'kafka:DescribeCluster', 'kafka:DescribeClusterV2'],
      resources: [mskStack.clusterArn],
    }));

    // VPC networking permissions for MSK Connect
    this.executionRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'ec2:CreateNetworkInterface',
        'ec2:DescribeNetworkInterfaces',
        'ec2:DescribeVpcs',
        'ec2:DeleteNetworkInterface',
        'ec2:DescribeSubnets',
        'ec2:DescribeSecurityGroups',
      ],
      resources: ['*'],
    }));

    const privateSubnetIds = vpcStack.vpc
      .selectSubnets({ subnetType: cdk.aws_ec2.SubnetType.PRIVATE_WITH_EGRESS })
      .subnetIds;

    new cdk.CfnOutput(this, 'PluginBucketName', {
      value: this.pluginBucket.bucketName,
      exportName: 'DataStreamPluginBucket',
      description: 'Upload connector ZIPs here before running setup-connectors.sh',
    });
    new cdk.CfnOutput(this, 'MskConnectRoleArn', {
      value: this.executionRole.roleArn,
      exportName: 'DataStreamMskConnectRoleArn',
      description: 'IAM role ARN for MSK Connect connectors',
    });
    new cdk.CfnOutput(this, 'ConnectLogGroup', {
      value: connectLogGroup.logGroupName,
      description: 'CloudWatch log group for MSK Connect worker logs',
    });
    new cdk.CfnOutput(this, 'PrivateSubnets', {
      value: cdk.Fn.join(',', privateSubnetIds),
      exportName: 'DataStreamPrivateSubnets',
      description: 'Private subnet IDs for MSK Connect worker placement',
    });
    new cdk.CfnOutput(this, 'MskConnectSgId', {
      value: vpcStack.sgMskConnect.securityGroupId,
      exportName: 'DataStreamMskConnectSgId',
      description: 'Security group ID for MSK Connect workers',
    });
  }
}
