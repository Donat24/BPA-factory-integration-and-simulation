// Lambda Function to Decode Hex message and write to DB
// Loads in the AWS SDK
const AWS = require('aws-sdk'); 


// Decode hex message
function DecodeMsg(bytes) {
    var machine = bytes[0];
    var message_type = bytes[1];
    var message = bytes[2];
    var timestamp = (bytes[3] << 24 | bytes[4] << 16 || bytes[5] << 8 | bytes[6] );
    
    var iot_bottle_msg = {
      "machine": machine,
      "message_type" : message_type,
      "message" : message,
      "timestamp" : timestamp,
    };
    
    return {
        iot_bottle_msg
    }
}

function WriteToDB(data, requestId) {
    // Creates the document client specifing the region 
    const ddb = new AWS.DynamoDB.DocumentClient({region: 'us-east-1'}); 
    
    const params = {
        TableName: 'bpa-prak-stuff',
        Item: {
          "awsRequestId": requestId,
          "machine": data.machine,
          "message_type" : data.message_type,
          "message" : data.message,
          "timestamp" : data.timestamp
        }
    }
    return ddb.put(params).promise();
}

exports.handler = async (event, context) => {
    
    const buf = Buffer.from(event, 'hex');
    var decodedMsg = DecodeMsg(buf);
    var dbwriter = WriteToDB(decodedMsg.iot_bottle_msg, context.awsRequestId)
    
    return dbwriter;
};