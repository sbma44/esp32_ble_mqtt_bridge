import boto3
from troposphere import Ref, Template, Tags, Join, Output, GetAtt
from awacs.aws import Allow, Policy, Principal, Statement, Action
import troposphere.iam

# S3_BUCKET='sbma44'
# S3_PATH='50q/sensors/'
try:
    from local_settings import *
except:
    pass

t = Template()
t.set_description('User for uploading temp/humidity data')

s3policy = troposphere.iam.Policy(
    PolicyName='EnvSensorS3Policy',
    PolicyDocument= Policy(
        Statement=[
            Statement(
                Sid='S3PutGetList',
                Effect=Allow,
                Action=[
                    Action('s3', 'Get*'),
                    Action('s3', 'Put*')
                ],
                Resource=['arn:aws:s3:::{}/{}*'.format(S3_BUCKET, S3_PATH)]
            )
        ]
    )
)

sensor_user = t.add_resource(troposphere.iam.User('EnvSensorUser', Policies=[s3policy]))

sensor_user_keys = t.add_resource(troposphere.iam.AccessKey(
    "EnvSensorUserKeys",
    Status="Active",
    UserName=Ref(sensor_user))
)

# add output to template
t.add_output(Output(
    "AccessKey",
    Value=Ref(sensor_user_keys),
    Description="AWSAccessKeyId",
))
t.add_output(Output(
    "SecretKey",
    Value=GetAtt(sensor_user_keys, "SecretAccessKey"),
    Description="AWSSecretKey",
))

template_json = t.to_json(indent=4)
cfn = boto3.client('cloudformation')
cfn.validate_template(TemplateBody=template_json)

stack ={}
stack['StackName'] = 'EnvSensorS3User'
stack['TemplateBody'] = template_json
stack['Capabilities'] = ['CAPABILITY_NAMED_IAM']


stack_exists = False
stacks = cfn.list_stacks()['StackSummaries']
for s in stacks:
    if s['StackStatus'] == 'DELETE_COMPLETE':
        continue
    if s['StackName'] == stack['StackName']:
        stack_exists = True

if stack_exists:
    print('Updating {}'.format(stack['StackName']))
    stack_result = cfn.update_stack(**stack)
    waiter = cfn.get_waiter('stack_update_complete')
else:
    print('Creating {}'.format(stack['StackName']))
    stack_result = cfn.create_stack(**stack)
    waiter = cfn.get_waiter('stack_create_complete')
print("...waiting for stack to be ready...")
waiter.wait(StackName=stack['StackName'])
