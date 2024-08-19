import { 
  SQSClient, 
  SendMessageCommand, 
  ReceiveMessageCommand, 
  DeleteMessageCommand, 
  GetQueueAttributesCommand, 
  QueueAttributeName,
  SendMessageBatchCommand, 
  PurgeQueueCommand,
  DeleteMessageBatchCommand 
} from '@aws-sdk/client-sqs';

import type { Message } from "@aws-sdk/client-sqs";

class AwsSQS {
private sqsClient: SQSClient;
private defaultQueueUrl: string;

/**
 * Constructs the AwsSQS instance.
 * 
 * @param region The AWS region for the SQS client.
 * @param defaultQueueUrl The default URL of the Amazon SQS queue.
 */
constructor(region: string, defaultQueueUrl: string) {
    this.sqsClient = new SQSClient({ region: region });
    this.defaultQueueUrl = defaultQueueUrl;
}

/**
 * Sends a SQL query to the specified SQS queue.
 * 
 * @param query SQL query to be sent.
 * @param parameters Parameters for the query.
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves when the message is sent successfully.
 * @throws Will throw an error if the send operation fails.
 */
async sendSqlQuery(query: string, parameters: Array<string | number>, queueUrl?: string): Promise<void> {
    const messageBody = JSON.stringify({ query, parameters });
    const command = new SendMessageCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl,
        MessageBody: messageBody
    });

    try {
        const response = await this.sqsClient.send(command);
        console.log('SQL query sent successfully, MessageId:', response.MessageId);
    } catch (error) {
        console.error('Failed to send SQL query:', error);
        throw error;
    }
}

/**
 * Receives messages from the specified SQS queue.
 * 
 * @param maxMessages The maximum number of messages to return. Default is 10.
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves to an array of received messages.
 * @throws Will throw an error if the receive operation fails.
 */
async receiveMessages(maxMessages: number = 10, queueUrl?: string): Promise<Message[]> {
    const command = new ReceiveMessageCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl,
        MaxNumberOfMessages: maxMessages
    });

    try {
        const response = await this.sqsClient.send(command);
        return response.Messages || [];
    } catch (error) {
        console.error('Failed to receive messages:', error);
        throw error;
    }
}

/**
 * Deletes a message from the specified SQS queue.
 * 
 * @param receiptHandle The receipt handle associated with the message to delete.
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves when the message is deleted successfully.
 * @throws Will throw an error if the delete operation fails.
 */
async deleteMessage(receiptHandle: string, queueUrl?: string): Promise<void> {
    const command = new DeleteMessageCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl,
        ReceiptHandle: receiptHandle
    });

    try {
        await this.sqsClient.send(command);
        console.log('Message deleted successfully');
    } catch (error) {
        console.error('Failed to delete message:', error);
        throw error;
    }
}

/**
 * Retrieves attributes for the specified SQS queue.
 * 
 * @param attributeNames An array of attribute names to retrieve. Default is ['All'].
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves to an object containing the requested queue attributes.
 * @throws Will throw an error if the get attributes operation fails.
 */
async getQueueAttributes(attributeNames: QueueAttributeName[] = ['All'], queueUrl?: string): Promise<Record<string, string>> {
    const command = new GetQueueAttributesCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl,
        AttributeNames: attributeNames
    });

    try {
        const response = await this.sqsClient.send(command);
        return response.Attributes || {};
    } catch (error) {
        console.error('Failed to get queue attributes:', error);
        throw error;
    }
}

/**
 * Sends multiple messages in a single batch to the specified SQS queue.
 * 
 * @param messages An array of message objects, each containing a query and parameters.
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves when the batch is sent successfully.
 * @throws Will throw an error if the send batch operation fails.
 */
async sendBatchMessages(messages: Array<{ query: string, parameters: Array<string | number> }>, queueUrl?: string): Promise<void> {
    const entries = messages.map((msg, index) => ({
        Id: `msg${index}`,
        MessageBody: JSON.stringify(msg)
    }));

    const command = new SendMessageBatchCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl,
        Entries: entries
    });

    try {
        await this.sqsClient.send(command);
        console.log('Batch messages sent successfully');
    } catch (error) {
        console.error('Failed to send batch messages:', error);
        throw error;
    }
}

/**
 * Deletes multiple messages in a single batch from the specified SQS queue.
 * 
 * @param entries An array of objects, each containing the message ID and receipt handle to delete.
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves to an object containing successful and failed message deletions.
 * @throws Will throw an error if the delete batch operation fails.
 */
async deleteBatchMessages(entries: Array<{ id: string, receiptHandle: string }>, queueUrl?: string): Promise<{
    successful: Array<{ id: string }>,
    failed: Array<{ id: string, code: string, message: string }>
}> {
    const deleteEntries = entries.map(entry => ({
        Id: entry.id,
        ReceiptHandle: entry.receiptHandle
    }));

    const command = new DeleteMessageBatchCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl,
        Entries: deleteEntries
    });

    try {
        const response = await this.sqsClient.send(command);
        console.log('Batch messages deleted successfully');
        return {
            successful: response.Successful?.map(entry => ({ id: entry.Id ?? '' })) || [],
            failed: response.Failed?.map(entry => ({ 
                id: entry.Id ?? '', 
                code: entry.Code ?? '', 
                message: entry.Message ?? '' 
            })) || []
        };
    } catch (error) {
        console.error('Failed to delete batch messages:', error);
        throw error;
    }
}

/**
 * Deletes all messages in the specified SQS queue.
 * 
 * @param queueUrl Optional queue URL. If not provided, the default queue URL will be used.
 * @returns A promise that resolves when the queue is purged successfully.
 * @throws Will throw an error if the purge operation fails.
 */
async purgeQueue(queueUrl?: string): Promise<void> {
    const command = new PurgeQueueCommand({
        QueueUrl: queueUrl || this.defaultQueueUrl
    });

    try {
        await this.sqsClient.send(command);
        console.log('Queue purged successfully');
    } catch (error) {
        console.error('Failed to purge queue:', error);
        throw error;
    }
}
}

export default AwsSQS;