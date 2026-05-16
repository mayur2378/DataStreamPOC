#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { VpcStack } from '../lib/vpc-stack';
import { RdsStack } from '../lib/rds-stack';
import { MskStack } from '../lib/msk-stack';
import { ConnectorsStack } from '../lib/connectors-stack';

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const vpcStack = new VpcStack(app, 'DataStreamVpcStack', { env });

const rdsStack = new RdsStack(app, 'DataStreamRdsStack', {
  env,
  vpcStack,
});
rdsStack.addDependency(vpcStack);

const mskStack = new MskStack(app, 'DataStreamMskStack', {
  env,
  vpcStack,
});
mskStack.addDependency(vpcStack);

const connectorsStack = new ConnectorsStack(app, 'DataStreamConnectorsStack', {
  env,
  vpcStack,
  rdsStack,
  mskStack,
});
connectorsStack.addDependency(mskStack);
connectorsStack.addDependency(rdsStack);
