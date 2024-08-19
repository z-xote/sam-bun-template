import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command, HeadObjectCommand, CopyObjectCommand } from '@aws-sdk/client-s3';
import mysql from 'mysql2/promise';


/**
 * @class AwsS3
 * @classdesc Provides methods for interacting with AWS S3, including metadata operations.
 */
export class AwsS3 {
  private s3Client: S3Client;
  private bucket: string;

  /**
   * Initializes an instance of the AwsS3 class.
   * @param {string} bucket - The name of the S3 bucket.
   * @param {string} [region] - The AWS region. Defaults to process.env.S3_REGION or 'us-west-2'.
   */
  constructor(bucket: string, region: string = process.env.S3_REGION || 'us-west-2') {
    this.s3Client = new S3Client({ region });
    this.bucket = bucket;
  }

  private async streamToBuffer(stream: any): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const chunks: any[] = [];
      stream.on('data', (chunk: any) => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
  }

  /**
   * Downloads a file from S3.
   * @param {string} key - The key of the file to download.
   * @returns {Promise<Buffer>} The content of the file.
   */
  async downloadFile(key: string): Promise<Buffer> {
    try {
      const command = new GetObjectCommand({ Bucket: this.bucket, Key: key });
      const response = await this.s3Client.send(command);
      
      if (!response.Body) {
        throw new Error('Empty response body');
      }

      return await this.streamToBuffer(response.Body);
    } catch (error) {
      console.error('Error downloading file from S3:', error);
      throw error;
    }
  }

  /**
   * Uploads a file to S3 with optional metadata.
   * @param {string} key - The key of the file to upload.
   * @param {Buffer} fileBuffer - The content of the file.
   * @param {string} contentType - The MIME type of the file.
   * @param {Record<string, string>} [metadata] - Optional metadata to attach to the file.
   * @returns {Promise<void>}
   */
  async uploadFile(key: string, fileBuffer: Buffer, contentType: string = 'application/octet-stream', metadata?: Record<string, string>): Promise<void> {
    try {
      const command = new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: fileBuffer,
        ContentType: contentType,
        Metadata: metadata,
      });
      await this.s3Client.send(command);
    } catch (error) {
      console.error('Error uploading file to S3:', error);
      throw error;
    }
  }

  /**
   * Deletes a file from S3.
   * @param {string} key - The key of the file to delete.
   * @returns {Promise<void>}
   */
  async deleteFile(key: string): Promise<void> {
    try {
      const command = new DeleteObjectCommand({
        Bucket: this.bucket,
        Key: key,
      });
      await this.s3Client.send(command);
    } catch (error) {
      console.error('Error deleting file from S3:', error);
      throw error;
    }
  }

  /**
   * Checks if a file exists in S3.
   * @param {string} key - The key of the file to check.
   * @returns {Promise<boolean>} True if the file exists, false otherwise.
   */
  async fileExists(key: string): Promise<boolean> {
    try {
      const command = new HeadObjectCommand({ Bucket: this.bucket, Key: key });
      await this.s3Client.send(command);
      return true;
    } catch (error) {
      if ((error as any).name === 'NotFound') {
        return false;
      }
      throw error;
    }
  }

  /**
   * Lists files in S3 with a specified prefix.
   * @param {string} prefix - The prefix of the files to list.
   * @returns {Promise<string[]>} A list of keys for the files that match the prefix.
   */
  async listFiles(prefix: string = ''): Promise<string[]> {
    try {
      const command = new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: prefix,
      });
      const response = await this.s3Client.send(command);
      return response.Contents?.map(file => file.Key || '') || [];
    } catch (error) {
      console.error('Error listing files in S3:', error);
      throw error;
    }
  }

  /**
   * Fetches metadata for an S3 object.
   * @param {string} key - The key of the file to fetch metadata for.
   * @returns {Promise<Record<string, string>>} The metadata of the file.
   */
  async getMetadata(key: string): Promise<Record<string, string>> {
    try {
      const command = new HeadObjectCommand({ Bucket: this.bucket, Key: key });
      const response = await this.s3Client.send(command);
      return response.Metadata || {};
    } catch (error) {
      console.error('Error fetching metadata from S3:', error);
      throw error;
    }
  }

  /**
  /**
   * Updates metadata for an existing S3 object.
   * @param {string} key - The key of the file to update metadata for.
   * @param {Record<string, string>} metadata - The new metadata to set.
   * @returns {Promise<void>}
   */
  async updateMetadata(key: string, metadata: Record<string, string>): Promise<void> {
    try {
      const headCommand = new HeadObjectCommand({ Bucket: this.bucket, Key: key });
      const headResponse = await this.s3Client.send(headCommand);

      const copyCommand = new CopyObjectCommand({
        Bucket: this.bucket,
        Key: key,
        CopySource: `${this.bucket}/${key}`,
        Metadata: { ...headResponse.Metadata, ...metadata },
        MetadataDirective: 'REPLACE',
      });

      await this.s3Client.send(copyCommand);
    } catch (error) {
      console.error('Error updating metadata in S3:', error);
      throw error;
    }
  }
}

// Example usage:
// const s3 = new AwsS3('my-bucket');
// 
// // Download a file
// const fileContent = await s3.downloadFile('path/to/file.txt');
// 
// // Upload a file with metadata
// await s3.uploadFile('path/to/newfile.txt', Buffer.from('Hello, S3!'), 'text/plain', { 'custom-meta': 'value' });
// 
// // Get metadata
// const metadata = await s3.getMetadata('path/to/file.txt');
// 
// // Update metadata
// await s3.updateMetadata('path/to/file.txt', { 'new-meta': 'new-value' });
// 
// // Other operations remain the same


type QueryResult<T> = T extends mysql.RowDataPacket[][] ? T[number] : T;

export class DBConnect {
  private connection!: mysql.Connection;

  constructor(private readonly connectionString: string) {}

  async connect(): Promise<void> {
    if (this.connection) {
      return;
    }

    const url = new URL(this.connectionString);
    this.connection = await mysql.createConnection({
      host: url.hostname,
      port: parseInt(url.port, 10) || 3306,
      user: url.username,
      password: url.password,
      database: url.pathname.substring(1),
      namedPlaceholders: true,
    });
  }

  async queryDb<T = mysql.RowDataPacket>(
    sql: string,
    params: Record<string, any> = {}
  ): Promise<QueryResult<T>> {
    await this.connect();
    const [results] = await this.connection.execute<mysql.ResultSetHeader[]>(sql, params);
    return results as QueryResult<T>;
  }

  async transaction<T>(callback: (connection: mysql.Connection) => Promise<T>): Promise<T> {
    await this.connect();
    await this.connection.beginTransaction();
    try {
      const result = await callback(this.connection);
      await this.connection.commit();
      return result;
    } catch (error) {
      await this.connection.rollback();
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.end();
    }
  }
}