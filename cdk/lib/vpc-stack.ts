import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';

export class VpcStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly sgRds: ec2.SecurityGroup;
  public readonly sgMsk: ec2.SecurityGroup;
  public readonly sgMskConnect: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // devIp restricts direct DB access from the developer's machine.
    // Pass --context devIp=<your-ip>/32 at deploy time.
    const devIp = this.node.tryGetContext('devIp') as string | undefined;

    this.vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    // RDS security group: allow developer laptop + MSK Connect workers
    this.sgRds = new ec2.SecurityGroup(this, 'SgRds', {
      vpc: this.vpc,
      description: 'RDS PostgreSQL access',
      allowAllOutbound: true,
    });
    if (devIp) {
      this.sgRds.addIngressRule(ec2.Peer.ipv4(devIp), ec2.Port.tcp(5432), 'Developer IP');
    } else {
      // Warn-only: remove this for production; kept open for POC convenience
      this.sgRds.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(5432), 'Open for POC - restrict with --context devIp');
    }

    // MSK security group: Kafka plaintext port from MSK Connect workers only
    this.sgMsk = new ec2.SecurityGroup(this, 'SgMsk', {
      vpc: this.vpc,
      description: 'MSK broker access',
      allowAllOutbound: true,
    });

    // MSK Connect worker security group: egress to MSK and RDS
    this.sgMskConnect = new ec2.SecurityGroup(this, 'SgMskConnect', {
      vpc: this.vpc,
      description: 'MSK Connect worker instances',
      allowAllOutbound: true,
    });

    // Cross-group ingress rules
    this.sgMsk.addIngressRule(this.sgMskConnect, ec2.Port.tcp(9092), 'MSK Connect to Kafka plaintext');
    this.sgRds.addIngressRule(this.sgMskConnect, ec2.Port.tcp(5432), 'MSK Connect to RDS');

    new cdk.CfnOutput(this, 'VpcId', { value: this.vpc.vpcId });
  }
}
