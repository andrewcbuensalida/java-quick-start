package com.mongodb.quickstart.transactions;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationOptions;
import org.bson.BsonDocument;

import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP;

public class ChangeStreams {

    private static final String CART = "cart";
    private static final String PRODUCT = "product";

    public static void main(String[] args) {
        ConnectionString connectionString = new ConnectionString(System.getProperty("mongodb.uri"));
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                                                                .applyConnectionString(connectionString)
                                                                .build();
        try (MongoClient client = MongoClients.create(clientSettings)) {
            MongoDatabase db = client.getDatabase("test");
            System.out.println("Dropping the '" + db.getName() + "' database.");
            db.drop();
            System.out.println("Creating the '" + CART + "'  collection.");
            db.createCollection(CART);
            System.out.println("Creating the '" + PRODUCT + "' collection with a JSON Schema.");
            db.createCollection(PRODUCT, productJsonSchemaValidator());
            System.out.println("Watching the collections in the DB " + db.getName() + "...");
            db.watch()
              .fullDocument(UPDATE_LOOKUP)
              .forEach(doc -> System.out.println(doc.getClusterTime() + " => " + doc.getFullDocument()));
        }
    }

    private static CreateCollectionOptions productJsonSchemaValidator() {
        String jsonSchema = "{\n" +
                "  \"$jsonSchema\": {\n" +
                "    \"bsonType\": \"object\",\n" +
                "    \"required\": [\"_id\", \"price\", \"stock\"],\n" +
                "    \"properties\": {\n" +
                "      \"_id\": {\n" +
                "        \"bsonType\": \"string\",\n" +
                "        \"description\": \"must be a string and is required\"\n" +
                "      },\n" +
                "      \"price\": {\n" +
                "        \"bsonType\": \"decimal\",\n" +
                "        \"minimum\": 0,\n" +
                "        \"description\": \"must be a non-negative decimal and is required\"\n" +
                "      },\n" +
                "      \"stock\": {\n" +
                "        \"bsonType\": \"int\",\n" +
                "        \"minimum\": 0,\n" +
                "        \"description\": \"must be a non-negative integer and is required\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        return new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validationAction(ValidationAction.ERROR)
                                       .validator(BsonDocument.parse(jsonSchema)));
    }
}
