// index.ts
import { AwsS3 } from './tools';


interface MyEvent {
    s3Path: string;
    requestId: string;
    chunkDuration: number;
} // what we're passed in

interface MyResult {
    statusCode: number;
    body: {
        message: string;
        chunks?: { s3Path: string; startTime: number }[]; // Updated to include startTime
    };
} // what we expect to return from lambda

const S3_BUCKET = process.env.S3_BUCKET || 'ws-upload-nicourt';


const s3 = new AwsS3(S3_BUCKET);

export const handler = async (event: MyEvent): Promise<MyResult> => {

  // make sure to return type myresult.

}
