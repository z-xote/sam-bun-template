import {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
  ConditionalCheckFailedException,
  ResourceNotFoundException,
  AttributeValue
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import winston from 'winston';

// Configure Winston Logger
const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ level, message, timestamp }) => {
      return `${timestamp} ${level}: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console()
  ]
});

class AwsDynamoDB {
  private client: DynamoDBClient;
  private defaultTableName: string;

  constructor(region: string, defaultTableName: string) {
    this.client = new DynamoDBClient({ region });
    this.defaultTableName = defaultTableName;
    logger.debug(`AwsDynamoDB initialized with region: ${region}, default table name: ${defaultTableName}`);
  }

  private getTableName(tableName?: string): string {
    const resolvedTableName = tableName || this.defaultTableName;
    logger.debug(`Resolved table name: ${resolvedTableName}`);
    return resolvedTableName;
  }

  async putItem(item: Record<string, any>, tableName?: string): Promise<void> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Putting item into table: ${resolvedTableName}`);
    logger.debug(`Item to put: ${JSON.stringify(item)}`);

    const marshalledItem = marshall(item);
    logger.debug(`Marshalled item: ${JSON.stringify(marshalledItem)}`);

    const command = new PutItemCommand({
      TableName: resolvedTableName,
      Item: marshalledItem
    });

    try {
      logger.debug(`Sending PutItemCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`PutItemCommand response: ${JSON.stringify(response)}`);
    } catch (error) {
      logger.error(`Error putting item into table ${resolvedTableName}:`, error);
      throw error;
    }
  }

  async getItem(key: Record<string, any>, tableName?: string): Promise<Record<string, any> | null> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Getting item from table: ${resolvedTableName}`);
    logger.debug(`Key to get: ${JSON.stringify(key)}`);

    const command = new GetItemCommand({
      TableName: resolvedTableName,
      Key: marshall(key)
    });

    try {
      logger.debug(`Sending GetItemCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`GetItemCommand response: ${JSON.stringify(response)}`);
      return response.Item ? unmarshall(response.Item) : null;
    } catch (error) {
      if (error instanceof ResourceNotFoundException) {
        logger.warn(`Item not found in table ${resolvedTableName}`);
        return null;
      }
      logger.error(`Error getting item from table ${resolvedTableName}:`, error);
      throw error;
    }
  }

  async updateItem(
    key: Record<string, any>,
    updateExpression: string,
    expressionAttributeValues: Record<string, any>,
    conditionExpression?: string,
    tableName?: string
  ): Promise<Record<string, any> | null> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Updating item in table: ${resolvedTableName}`);
    logger.debug(`Key to update: ${JSON.stringify(key)}`);
    logger.debug(`Update expression: ${updateExpression}`);
    logger.debug(`Expression attribute values: ${JSON.stringify(expressionAttributeValues)}`);
    logger.debug(`Condition expression: ${conditionExpression}`);

    const command = new UpdateItemCommand({
      TableName: resolvedTableName,
      Key: marshall(key),
      UpdateExpression: updateExpression,
      ExpressionAttributeValues: marshall(expressionAttributeValues),
      ConditionExpression: conditionExpression,
      ReturnValues: 'ALL_NEW'
    });

    try {
      logger.debug(`Sending UpdateItemCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`UpdateItemCommand response: ${JSON.stringify(response)}`);
      return response.Attributes ? unmarshall(response.Attributes) : null;
    } catch (error) {
      if (error instanceof ConditionalCheckFailedException) {
        logger.warn(`Conditional check failed for update in table ${resolvedTableName}`);
        return null;
      }
      logger.error(`Error updating item in table ${resolvedTableName}:`, error);
      throw error;
    }
  }
  async deleteItem(key: Record<string, any>, tableName?: string): Promise<void> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Deleting item from table: ${resolvedTableName}`);
    logger.debug(`Key to delete: ${JSON.stringify(key)}`);

    const command = new DeleteItemCommand({
      TableName: resolvedTableName,
      Key: marshall(key)
    });

    try {
      logger.debug(`Sending DeleteItemCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`DeleteItemCommand response: ${JSON.stringify(response)}`);
    } catch (error) {
      logger.error(`Error deleting item from table ${resolvedTableName}:`, error);
      throw error;
    }
  }

  async query(
    keyConditionExpression: string,
    expressionAttributeValues: Record<string, any>,
    filterExpression?: string,
    tableName?: string
  ): Promise<Record<string, any>[]> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Querying table: ${resolvedTableName}`);
    logger.debug(`Key condition expression: ${keyConditionExpression}`);
    logger.debug(`Expression attribute values: ${JSON.stringify(expressionAttributeValues)}`);
    logger.debug(`Filter expression: ${filterExpression}`);

    const command = new QueryCommand({
      TableName: resolvedTableName,
      KeyConditionExpression: keyConditionExpression,
      ExpressionAttributeValues: marshall(expressionAttributeValues),
      FilterExpression: filterExpression
    });

    try {
      logger.debug(`Sending QueryCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`QueryCommand response: ${JSON.stringify(response)}`);
      return response.Items ? response.Items.map(item => unmarshall(item)) : [];
    } catch (error) {
      logger.error(`Error querying table ${resolvedTableName}:`, error);
      throw error;
    }
  }

  async scan(
    filterExpression?: string,
    expressionAttributeValues?: Record<string, any>,
    tableName?: string
  ): Promise<Record<string, any>[]> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Scanning table: ${resolvedTableName}`);
    logger.debug(`Filter expression: ${filterExpression}`);
    logger.debug(`Expression attribute values: ${JSON.stringify(expressionAttributeValues)}`);

    const command = new ScanCommand({
      TableName: resolvedTableName,
      FilterExpression: filterExpression,
      ExpressionAttributeValues: expressionAttributeValues ? marshall(expressionAttributeValues) : undefined
    });

    try {
      logger.debug(`Sending ScanCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`ScanCommand response: ${JSON.stringify(response)}`);
      return response.Items ? response.Items.map(item => unmarshall(item)) : [];
    } catch (error) {
      logger.error(`Error scanning table ${resolvedTableName}:`, error);
      throw error;
    }
  }

  async transactWrite(
    operations: Array<{
      type: 'Put' | 'Update' | 'Delete' | 'ConditionCheck',
      item?: Record<string, any>,
      key?: Record<string, any>,
      updateExpression?: string,
      conditionExpression?: string,
      expressionAttributeValues?: Record<string, any>
    }>,
    tableName?: string
  ): Promise<void> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Performing transact write on table: ${resolvedTableName}`);
    logger.debug(`Operations: ${JSON.stringify(operations)}`);

    const transactItems = operations.map(op => {
      const table = resolvedTableName;
      switch (op.type) {
        case 'Put':
          return { Put: { TableName: table, Item: marshall(op.item!) } };
        case 'Update':
          return {
            Update: {
              TableName: table,
              Key: marshall(op.key!),
              UpdateExpression: op.updateExpression,
              ConditionExpression: op.conditionExpression,
              ExpressionAttributeValues: op.expressionAttributeValues ? marshall(op.expressionAttributeValues) : undefined
            }
          };
        case 'Delete':
          return {
            Delete: {
              TableName: table,
              Key: marshall(op.key!),
              ConditionExpression: op.conditionExpression,
              ExpressionAttributeValues: op.expressionAttributeValues ? marshall(op.expressionAttributeValues) : undefined
            }
          };
        case 'ConditionCheck':
          return {
            ConditionCheck: {
              TableName: table,
              Key: marshall(op.key!),
              ConditionExpression: op.conditionExpression!,
              ExpressionAttributeValues: op.expressionAttributeValues ? marshall(op.expressionAttributeValues) : undefined
            }
          };
        default:
          throw new Error(`Unsupported operation type: ${op.type}`);
      }
    });

    const command = new TransactWriteItemsCommand({ TransactItems: transactItems });

    try {
      logger.debug(`Sending TransactWriteItemsCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`TransactWriteItemsCommand response: ${JSON.stringify(response)}`);
    } catch (error) {
      logger.error(`Error in transact write on table ${resolvedTableName}:`, error);
      throw error;
    }
  }

  async transactGet(
    gets: Array<{ key: Record<string, any> }>,
    tableName?: string
  ): Promise<Record<string, any>[]> {
    const resolvedTableName = this.getTableName(tableName);
    logger.debug(`Performing transact get on table: ${resolvedTableName}`);
    logger.debug(`Gets: ${JSON.stringify(gets)}`);

    const transactItems = gets.map(get => ({
      Get: {
        TableName: resolvedTableName,
        Key: marshall(get.key)
      }
    }));

    const command = new TransactGetItemsCommand({ TransactItems: transactItems });

    try {
      logger.debug(`Sending TransactGetItemsCommand to DynamoDB`);
      const response = await this.client.send(command);
      logger.debug(`TransactGetItemsCommand response: ${JSON.stringify(response)}`);
      return response.Responses ? response.Responses.map(item =>
        unmarshall(item.Item as Record<string, AttributeValue>)
      ) : [];
    } catch (error) {
      logger.error(`Error in transact get on table ${resolvedTableName}:`, error);
      throw error;
    }
  }
}

export default AwsDynamoDB;