export interface NavChild {
  label: string
  path: string
}

export interface NavSection {
  id: string
  label: string
  basePath: string
  /** Navigate here when the section header is clicked. */
  rootPath: string
  children: NavChild[]
}

/** AWS Console category for each service, in console display order. */
export const categoryOrder = [
  'Compute',
  'Containers',
  'Storage',
  'Database',
  'Networking & Content Delivery',
  'Analytics',
  'Application Integration',
  'Management & Governance',
  'Security, Identity & Compliance',
  'Business Applications',
]

export const serviceCategory: Record<string, string> = {
  ec2: 'Compute',
  lambda: 'Compute',
  ecs: 'Containers',
  eks: 'Containers',
  s3: 'Storage',
  rds: 'Database',
  dynamodb: 'Database',
  elasticache: 'Database',
  route53: 'Networking & Content Delivery',
  elbv2: 'Networking & Content Delivery',
  apigateway: 'Networking & Content Delivery',
  emr: 'Analytics',
  'emr-containers': 'Analytics',
  glue: 'Analytics',
  kinesis: 'Analytics',
  firehose: 'Analytics',
  sqs: 'Application Integration',
  sns: 'Application Integration',
  eventbridge: 'Application Integration',
  sfn: 'Application Integration',
  cloudwatch: 'Management & Governance',
  logs: 'Management & Governance',
  cloudformation: 'Management & Governance',
  ssm: 'Management & Governance',
  iam: 'Security, Identity & Compliance',
  kms: 'Security, Identity & Compliance',
  secretsmanager: 'Security, Identity & Compliance',
  ses: 'Business Applications',
}

/** AWS service navigation, mirroring the AWS Console service menu. */
export const navTree: NavSection[] = [
  {
    id: 's3',
    label: 'S3',
    basePath: '/aws/s3',
    rootPath: '/aws/s3',
    children: [{ label: 'Buckets', path: '/aws/s3' }],
  },
  {
    id: 'dynamodb',
    label: 'DynamoDB',
    basePath: '/aws/dynamodb',
    rootPath: '/aws/dynamodb',
    children: [{ label: 'Tables', path: '/aws/dynamodb' }],
  },
  {
    id: 'sqs',
    label: 'SQS',
    basePath: '/aws/sqs',
    rootPath: '/aws/sqs',
    children: [{ label: 'Queues', path: '/aws/sqs' }],
  },
  {
    id: 'sns',
    label: 'SNS',
    basePath: '/aws/sns',
    rootPath: '/aws/sns',
    children: [{ label: 'Topics', path: '/aws/sns' }],
  },
  {
    id: 'lambda',
    label: 'Lambda',
    basePath: '/aws/lambda',
    rootPath: '/aws/lambda',
    children: [{ label: 'Functions', path: '/aws/lambda' }],
  },
  {
    id: 'logs',
    label: 'CloudWatch Logs',
    basePath: '/aws/logs',
    rootPath: '/aws/logs/groups',
    children: [
      { label: 'Log Groups', path: '/aws/logs/groups' },
      { label: 'Insights', path: '/aws/logs/insights' },
    ],
  },
  {
    id: 'cloudwatch',
    label: 'CloudWatch',
    basePath: '/aws/cloudwatch',
    rootPath: '/aws/cloudwatch/metrics',
    children: [
      { label: 'Metrics', path: '/aws/cloudwatch/metrics' },
      { label: 'Alarms', path: '/aws/cloudwatch/alarms' },
      { label: 'Dashboards', path: '/aws/cloudwatch/dashboards' },
    ],
  },
  {
    id: 'kms',
    label: 'KMS',
    basePath: '/aws/kms',
    rootPath: '/aws/kms',
    children: [{ label: 'Keys', path: '/aws/kms' }],
  },
  {
    id: 'secretsmanager',
    label: 'Secrets Manager',
    basePath: '/aws/secretsmanager',
    rootPath: '/aws/secretsmanager',
    children: [{ label: 'Secrets', path: '/aws/secretsmanager' }],
  },
  {
    id: 'ssm',
    label: 'SSM',
    basePath: '/aws/ssm',
    rootPath: '/aws/ssm',
    children: [{ label: 'Parameters', path: '/aws/ssm' }],
  },
  {
    id: 'iam',
    label: 'IAM',
    basePath: '/aws/iam',
    rootPath: '/aws/iam',
    children: [{ label: 'Roles', path: '/aws/iam' }],
  },
  {
    id: 'emr',
    label: 'EMR',
    basePath: '/aws/emr',
    rootPath: '/aws/emr/clusters',
    children: [{ label: 'Clusters', path: '/aws/emr/clusters' }],
  },
  {
    id: 'emr-containers',
    label: 'EMR on EKS',
    basePath: '/aws/emr-containers',
    rootPath: '/aws/emr-containers/clusters',
    children: [{ label: 'Virtual Clusters', path: '/aws/emr-containers/clusters' }],
  },
  {
    id: 'glue',
    label: 'Glue',
    basePath: '/aws/glue',
    rootPath: '/aws/glue/databases',
    children: [
      { label: 'Databases', path: '/aws/glue/databases' },
      { label: 'Jobs', path: '/aws/glue/jobs' },
      { label: 'Crawlers', path: '/aws/glue/crawlers' },
    ],
  },
  {
    id: 'eventbridge',
    label: 'EventBridge',
    basePath: '/aws/eventbridge',
    rootPath: '/aws/eventbridge/rules',
    children: [
      { label: 'Rules', path: '/aws/eventbridge/rules' },
      { label: 'Event Buses', path: '/aws/eventbridge/buses' },
    ],
  },
  {
    id: 'apigateway',
    label: 'API Gateway',
    basePath: '/aws/apigateway',
    rootPath: '/aws/apigateway/apis',
    children: [{ label: 'REST APIs', path: '/aws/apigateway/apis' }],
  },
  {
    id: 'sfn',
    label: 'Step Functions',
    basePath: '/aws/sfn',
    rootPath: '/aws/sfn/state-machines',
    children: [{ label: 'State Machines', path: '/aws/sfn/state-machines' }],
  },
  {
    id: 'ec2',
    label: 'EC2',
    basePath: '/aws/ec2',
    rootPath: '/aws/ec2/instances',
    children: [{ label: 'Instances', path: '/aws/ec2/instances' }],
  },
  {
    id: 'ecs',
    label: 'ECS',
    basePath: '/aws/ecs',
    rootPath: '/aws/ecs/clusters',
    children: [{ label: 'Clusters', path: '/aws/ecs/clusters' }],
  },
  {
    id: 'eks',
    label: 'EKS',
    basePath: '/aws/eks',
    rootPath: '/aws/eks/clusters',
    children: [{ label: 'Clusters', path: '/aws/eks/clusters' }],
  },
  {
    id: 'rds',
    label: 'RDS',
    basePath: '/aws/rds',
    rootPath: '/aws/rds/instances',
    children: [{ label: 'Instances', path: '/aws/rds/instances' }],
  },
  {
    id: 'elasticache',
    label: 'ElastiCache',
    basePath: '/aws/elasticache',
    rootPath: '/aws/elasticache/clusters',
    children: [{ label: 'Clusters', path: '/aws/elasticache/clusters' }],
  },
  {
    id: 'route53',
    label: 'Route 53',
    basePath: '/aws/route53',
    rootPath: '/aws/route53/zones',
    children: [{ label: 'Hosted Zones', path: '/aws/route53/zones' }],
  },
  {
    id: 'cloudformation',
    label: 'CloudFormation',
    basePath: '/aws/cloudformation',
    rootPath: '/aws/cloudformation/stacks',
    children: [{ label: 'Stacks', path: '/aws/cloudformation/stacks' }],
  },
  {
    id: 'kinesis',
    label: 'Kinesis',
    basePath: '/aws/kinesis',
    rootPath: '/aws/kinesis/streams',
    children: [{ label: 'Data Streams', path: '/aws/kinesis/streams' }],
  },
  {
    id: 'firehose',
    label: 'Firehose',
    basePath: '/aws/firehose',
    rootPath: '/aws/firehose/streams',
    children: [{ label: 'Delivery Streams', path: '/aws/firehose/streams' }],
  },
  {
    id: 'ses',
    label: 'SES',
    basePath: '/aws/ses',
    rootPath: '/aws/ses/identities',
    children: [{ label: 'Identities', path: '/aws/ses/identities' }],
  },
  {
    id: 'elbv2',
    label: 'Elastic Load Balancing',
    basePath: '/aws/elbv2',
    rootPath: '/aws/elbv2/load-balancers',
    children: [{ label: 'Load Balancers', path: '/aws/elbv2/load-balancers' }],
  },
]
