package dev.k1k.batchprocessor.dynamo;

import dev.k1k.batchprocessor.batch.InputObjProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import java.util.List;

import static dev.k1k.batchprocessor.dynamo.DynamoBatchWriteUtil.batchWrite;

@Component
@RequiredArgsConstructor
public class DynamoDBWriter {
    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;

    public void putBatchRecords(List<InputObjProcessor.OutputObj> objects) {
        DynamoDbTable<InputObjProcessor.OutputObj> outputMappedTable = dynamoDbEnhancedClient.table("output", TableSchema.fromBean(InputObjProcessor.OutputObj.class));
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_dynamodb_BatchWriteItem_section.html

        batchWrite(InputObjProcessor.OutputObj.class, objects, dynamoDbEnhancedClient, outputMappedTable);
    }


}
