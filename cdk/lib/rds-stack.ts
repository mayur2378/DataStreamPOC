import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { VpcStack } from './vpc-stack';

interface RdsStackProps extends cdk.StackProps {
  vpcStack: VpcStack;
}

export class RdsStack extends cdk.Stack {
  public readonly instance: rds.DatabaseInstance;
  public readonly secret: secretsmanager.ISecret;
  public readonly endpoint: string;

  constructor(scope: Construct, id: string, props: RdsStackProps) {
    super(scope, id, props);

    const { vpcStack } = props;

    // Logical replication is required by Debezium for CDC.
    const parameterGroup = new rds.ParameterGroup(this, 'PgParams', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_15,
      }),
      description: 'POC: logical replication for Debezium',
      parameters: {
        'rds.logical_replication': '1',
        max_replication_slots: '5',
        max_wal_senders: '5',
        wal_sender_timeout: '0',
      },
    });

    this.instance = new rds.DatabaseInstance(this, 'PocDb', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_15,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      vpc: vpcStack.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      securityGroups: [vpcStack.sgRds],
      publiclyAccessible: true,
      multiAz: false,
      allocatedStorage: 20,
      storageType: rds.StorageType.GP3,
      databaseName: 'pocdb',
      parameterGroup,
      deletionProtection: false,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      credentials: rds.Credentials.fromGeneratedSecret('pocadmin', {
        secretName: 'datastream-poc-db-secret',
      }),
    });

    this.secret = this.instance.secret!;
    this.endpoint = this.instance.instanceEndpoint.hostname;

    new cdk.CfnOutput(this, 'DbEndpoint', {
      value: this.endpoint,
      exportName: 'DataStreamDbEndpoint',
      description: 'RDS endpoint — use in psql -h and export DB_HOST',
    });
    new cdk.CfnOutput(this, 'DbSecretArn', {
      value: this.secret.secretArn,
      exportName: 'DataStreamDbSecretArn',
      description: 'export DB_SECRET_ARN=<value> before running inject.py',
    });
    new cdk.CfnOutput(this, 'DbName', {
      value: 'pocdb',
      exportName: 'DataStreamDbName',
    });
  }
}
