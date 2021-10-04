import boto3


def create_pelago_assignment_table(TableName):
    dynamodb = boto3.resource('dynamodb')
    dynamodb_client=boto3.client('dynamodb')


    existing_tables = dynamodb_client.list_tables()['TableNames']
    if TableName in existing_tables:
        print("Table already exist and not action taken")
        table=dynamodb.Table(TableName)
        return table;


    else:
        table = dynamodb.create_table(
        TableName=TableName,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'created',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'created',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


if __name__ == '__main__':
    pelago_assignment = create_pelago_assignment_table('palego_assignment')
    print("Table status:", pelago_assignment.table_status)
