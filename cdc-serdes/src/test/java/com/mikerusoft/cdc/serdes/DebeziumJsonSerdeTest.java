package com.mikerusoft.cdc.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DebeziumJsonSerdeTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Nested
    class KeyTests {

        @Test
        void whenKeyInSchemaIsIntAndValueIsInt_expectedInt() throws Exception {
            String keyWithInt  = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"}],\"optional\":false,\"name\":\"stamname.inventory.customers.Key\"},\"payload\":{\"id\":1002}}";
            DebeziumJsonSerde<Integer> serde = new DebeziumJsonSerde<>(mapper, Integer.class, true);
            Integer actual = serde.deserializer().deserialize("", mapper.writeValueAsBytes(mapper.readValue(keyWithInt, Map.class)));
            assertEquals(1002, actual);
        }

        @Test
        void whenKeyInSchemaIsStringAndValueIsInt_expectedInt() throws Exception {
            String keyWithInt  = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"}],\"optional\":false,\"name\":\"stamname.inventory.customers.Key\"},\"payload\":{\"id\":\"stam\"}}";
            DebeziumJsonSerde<String> serde = new DebeziumJsonSerde<>(mapper, String.class, true);
            String actual = serde.deserializer().deserialize("", mapper.writeValueAsBytes(mapper.readValue(keyWithInt, Map.class)));
            assertEquals("stam", actual);
        }

        @Test
        void whenValueNull_expectedNull() throws Exception {
            DebeziumJsonSerde<String> serde = new DebeziumJsonSerde<>(mapper, String.class, true);
            String actual = serde.deserializer().deserialize("", null);
            assertNull(actual);
        }

        @Test
        void whenValueEmptyByteArray_expectedNull() throws Exception {
            DebeziumJsonSerde<String> serde = new DebeziumJsonSerde<>(mapper, String.class, true);
            String actual = serde.deserializer().deserialize("", new byte[0]);
            assertNull(actual);
        }
    }

    @Nested
    class ValueTests {

        @Test
        void withPredicateEmptyCheck_whenDeleteEventValueContainsAllFieldsNull_expectedNull() throws Exception {
            String valueWith = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"email\"}],\"optional\":true,\"name\":\"stamname.inventory.customers.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"email\"}],\"optional\":true,\"name\":\"stamname.inventory.customers.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"stamname.inventory.customers.Envelope\"},\"payload\":{\"before\":{\"id\":1005,\"first_name\":\"Misha\",\"last_name\":\"Grinfeld\",\"email\":\"grinfeld@gmail.com\"},\"after\":null,\"source\":{\"version\":\"1.4.0.Alpha2\",\"connector\":\"mysql\",\"name\":\"stamname\",\"ts_ms\":1607930338000,\"snapshot\":\"false\",\"db\":\"inventory\",\"table\":\"customers\",\"server_id\":223344,\"gtid\":null,\"file\":\"mysql-bin.000006\",\"pos\":1019,\"row\":0,\"thread\":17,\"query\":null},\"op\":\"d\",\"ts_ms\":1607930338706,\"transaction\":null}}";
            DebeziumJsonSerde<Customer> serde = new DebeziumJsonSerde<>(mapper, Customer.class, false);
            Customer actual = serde.deserializer().deserialize("", mapper.writeValueAsBytes(mapper.readValue(valueWith, Map.class)));
            assertNull(actual);
        }

        @Test
        void whenUpdate_expectedUpdatedValue() throws Exception {
            String valueWith = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"email\"}],\"optional\":true,\"name\":\"stamname.inventory.customers.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"email\"}],\"optional\":true,\"name\":\"stamname.inventory.customers.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"stamname.inventory.customers.Envelope\"},\"payload\":{\"before\":{\"id\":1006,\"first_name\":\"Igor\",\"last_name\":\"Grinfeld\",\"email\":\"stam@gmail.com\"},\"after\":{\"id\":1006,\"first_name\":\"Igar\",\"last_name\":\"Grinfeld\",\"email\":\"stam@gmail.com\"},\"source\":{\"version\":\"1.4.0.Alpha2\",\"connector\":\"mysql\",\"name\":\"stamname\",\"ts_ms\":1607846383000,\"snapshot\":\"false\",\"db\":\"inventory\",\"table\":\"customers\",\"server_id\":223344,\"gtid\":null,\"file\":\"mysql-bin.000006\",\"pos\":677,\"row\":0,\"thread\":10,\"query\":null},\"op\":\"u\",\"ts_ms\":1607846383848,\"transaction\":null}}";
            DebeziumJsonSerde<Customer> serde = new DebeziumJsonSerde<>(mapper, Customer.class, false);
            Customer expected = new Customer(1006, "Igar", "Grinfeld", "stam@gmail.com");
            Customer actual = serde.deserializer().deserialize("", mapper.writeValueAsBytes(mapper.readValue(valueWith, Map.class)));
            assertEquals(expected, actual);
        }

        @Test
        void whenCreate_expectedUpdatedValue() throws Exception {
            String valueWith = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"email\"}],\"optional\":true,\"name\":\"stamname.inventory.customers.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"string\",\"optional\":false,\"field\":\"first_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"last_name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"email\"}],\"optional\":true,\"name\":\"stamname.inventory.customers.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"stamname.inventory.customers.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":1006,\"first_name\":\"Igor\",\"last_name\":\"Grinfeld\",\"email\":\"stam@gmail.com\"},\"source\":{\"version\":\"1.4.0.Alpha2\",\"connector\":\"mysql\",\"name\":\"stamname\",\"ts_ms\":1607845781000,\"snapshot\":\"false\",\"db\":\"inventory\",\"table\":\"customers\",\"server_id\":223344,\"gtid\":null,\"file\":\"mysql-bin.000006\",\"pos\":364,\"row\":0,\"thread\":10,\"query\":null},\"op\":\"c\",\"ts_ms\":1607845781362,\"transaction\":null}}";
            DebeziumJsonSerde<Customer> serde = new DebeziumJsonSerde<>(mapper, Customer.class, false);
            Customer expected = new Customer(1006, "Igor", "Grinfeld", "stam@gmail.com");
            Customer actual = serde.deserializer().deserialize("", mapper.writeValueAsBytes(mapper.readValue(valueWith, Map.class)));
            assertEquals(expected, actual);
        }
    }

    @Nested
    class SerializerTests {

        @Test
        void whenSerializer_throwsUnsupportedOperationException() {
            DebeziumJsonSerde<Customer> serde = new DebeziumJsonSerde<>(mapper, Customer.class, false);
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> serde.serializer().serialize("", new Customer())
            );
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Customer {
        private Integer id;
        private String first_name;
        private String last_name;
        private String email;
    }

}