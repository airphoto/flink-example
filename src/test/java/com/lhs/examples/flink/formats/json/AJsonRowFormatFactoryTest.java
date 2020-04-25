package com.lhs.examples.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.lhs.flink.example.java.sql.formats.json.AJsonRowDeserializationSchema;
import com.lhs.flink.example.java.sql.formats.json.AJsonRowFormatFactory;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import com.lhs.flink.example.java.sql.formats.descriptors.AJson;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link AJsonRowFormatFactory}.
 */
public class AJsonRowFormatFactoryTest {

	private static final String JSON_SCHEMA =
		"{" +
		"  'title': 'Fruit'," +
		"  'type': 'object'," +
		"  'properties': {" +
		"    'name': {" +
		"      'type': 'string'" +
		"    }," +
		"    'count': {" +
		"      'type': 'integer'" +
		"    }," +
		"    'time': {" +
		"      'description': 'row time'," +
		"      'type': 'string'," +
		"      'format': 'date-time'" +
		"    }" +
		"  }," +
		"  'required': ['name', 'count', 'time']" +
		"}";

	private static final TypeInformation<Row> SCHEMA = Types.ROW(
		new String[]{"field1", "field2"},
		new TypeInformation[]{Types.BOOLEAN(), Types.INT()});

	@Test
	public void testSchema() {
		AJson aJson = new AJson()
				.schema(SCHEMA)
				.failOnMissingField(false);

		final Map<String, String> properties = toMap(aJson);

//		testSchemaSerializationSchema(properties);

		testSchemaDeserializationSchema(properties);
	}

	@Test
	public void testJsonSchema() {
		final Map<String, String> properties = toMap(
			new AJson()
				.jsonSchema(JSON_SCHEMA)
				.failOnMissingField(true));

//		testJsonSchemaSerializationSchema(properties);

		testJsonSchemaDeserializationSchema(properties);
	}

	@Test
	public void testSchemaDerivation() {
		final Map<String, String> properties = toMap(
			new Schema()
				.field("field1", Types.BOOLEAN())
				.field("field2", Types.INT())
				.field("proctime", Types.SQL_TIMESTAMP()).proctime(),
			new AJson()
				.deriveSchema());

//		testSchemaSerializationSchema(properties);

		testSchemaDeserializationSchema(properties);
	}

	private void testSchemaDeserializationSchema(Map<String, String> properties) {
		final DeserializationSchema<?> actual2 = TableFactoryService
			.find(DeserializationSchemaFactory.class, properties)
			.createDeserializationSchema(properties);
		final AJsonRowDeserializationSchema expected2 = new AJsonRowDeserializationSchema.Builder(SCHEMA).build();
		assertEquals(expected2, actual2);
	}

//	private void testSchemaSerializationSchema(Map<String, String> properties) {
//		final SerializationSchema<?> actual1 = TableFactoryService
//			.find(SerializationSchemaFactory.class, properties)
//			.createSerializationSchema(properties);
//		final SerializationSchema<?> expected1 = new AJsonRowSerializationSchema.Builder(SCHEMA).build();
//		assertEquals(expected1, actual1);
//	}

	private void testJsonSchemaDeserializationSchema(Map<String, String> properties) {
		final DeserializationSchema<?> actual2 = TableFactoryService
			.find(DeserializationSchemaFactory.class, properties)
			.createDeserializationSchema(properties);
		final AJsonRowDeserializationSchema expected2 = new AJsonRowDeserializationSchema.Builder(JSON_SCHEMA)
			.failOnMissingField()
			.build();
		assertEquals(expected2, actual2);
	}

//	private void testJsonSchemaSerializationSchema(Map<String, String> properties) {
//		final SerializationSchema<?> actual1 = TableFactoryService
//			.find(SerializationSchemaFactory.class, properties)
//			.createSerializationSchema(properties);
//		final SerializationSchema<?> expected1 = new AJsonRowSerializationSchema.Builder(JSON_SCHEMA).build();
//		assertEquals(expected1, actual1);
//	}

	private static Map<String, String> toMap(Descriptor... desc) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		for (Descriptor d : desc) {
			descriptorProperties.putProperties(d.toProperties());
		}
		return descriptorProperties.asMap();
	}
}
