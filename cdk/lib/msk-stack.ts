import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as msk from 'aws-cdk-lib/aws-msk';
import * as logs from 'aws-cdk-lib/aws-logs';
import { VpcStack } from './vpc-stack';

interface MskStackProps extends cdk.StackProps {
  vpcStack: VpcStack;
}

export class MskStack extends cdk.Stack {
  public readonly cluster: msk.CfnCluster;
  public readonly clusterArn: string;

  constructor(scope: Construct, id: string, props: MskStackProps) {
    super(scope, id, props);

    const { vpcStack } = props;

    const logGroup = new logs.LogGroup(this, 'MskBrokerLogs', {
      logGroupName: '/datastream-poc/msk-broker',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Use private subnets — MSK Connect workers also run in these subnets.
    const privateSubnets = vpcStack.vpc.selectSubnets({
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    }).subnetIds;

    // MSK configuration: enable auto-create topics for POC convenience.
    const mskConfig = new msk.CfnConfiguration(this, 'MskConfig', {
      name: 'datastream-poc-config',
      serverProperties: [
        'auto.create.topics.enable=true',
        'default.replication.factor=2',
        'min.insync.replicas=1',
        'log.retention.hours=168',
        'num.partitions=3',
      ].join('\n'),
      kafkaVersionsList: ['3.6.0'],
    });

    // L1 CfnCluster gives full control over broker distribution and config.
    this.cluster = new msk.CfnCluster(this, 'PocMsk', {
      clusterName: 'datastream-poc',
      kafkaVersion: '3.6.0',
      numberOfBrokerNodes: 2,
      brokerNodeGroupInfo: {
        instanceType: 'kafka.t3.small',
        clientSubnets: privateSubnets.slice(0, 2),
        securityGroups: [vpcStack.sgMsk.securityGroupId],
        storageInfo: {
          ebsStorageInfo: { volumeSize: 20 },
        },
      },
      configurationInfo: {
        arn: mskConfig.ref,
        revision: 1,
      },
      encryptionInfo: {
        encryptionInTransit: {
          clientBroker: 'PLAINTEXT',
          inCluster: false,
        },
      },
      clientAuthentication: {
        unauthenticated: { enabled: true },
      },
      loggingInfo: {
        brokerLogs: {
          cloudWatchLogs: {
            enabled: true,
            logGroup: logGroup.logGroupName,
          },
        },
      },
    });
    this.cluster.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    this.clusterArn = this.cluster.ref;

    new cdk.CfnOutput(this, 'MskClusterArn', {
      value: this.clusterArn,
      exportName: 'DataStreamMskClusterArn',
      description: 'MSK cluster ARN — used by setup-connectors.sh',
    });
  }
}
