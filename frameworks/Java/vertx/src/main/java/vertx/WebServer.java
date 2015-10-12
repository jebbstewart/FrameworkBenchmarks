package vertx;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import freemarker.template.Template;
import freemarker.template.Configuration;


public class WebServer extends AbstractVerticle implements Handler<HttpServerRequest> {

	static Logger logger = LoggerFactory.getLogger(WebServer.class.getName());

	private final Random random = ThreadLocalRandom.current();

	private static final String PATH_PLAINTEXT = "/plaintext";
	private static final String PATH_JSON = "/json";
	private static final String PATH_DB = "/db";
	private static final String PATH_QUERIES = "/queries";
	private static final String PATH_UPDATES = "/updates";
	private static final String PATH_FORTUNES = "/fortunes";

	private static final String RESPONSE_TYPE_PLAIN = "text/plain";
	private static final String RESPONSE_TYPE_HTML = "text/html";
	private static final String RESPONSE_TYPE_JSON = "application/json";

	private static final String UNDERSCORE_ID = "_id";
	private static final String TEXT_ID = "id";
	private static final String RANDOM_NUMBER = "randomNumber";
	private static final String TEXT_QUERIES = "queries";
	private static final String TEXT_MESSAGE = "message";
	private static final String TEXT_MESSAGES = "messages";
	private static final String ADD_FORTUNE_MESSAGE = "Additional fortune added at request time.";
	private static final String HELLO_WORLD = "Hello, world!";
	private static final Buffer HELLO_WORLD_BUFFER = Buffer.buffer(HELLO_WORLD);

	private static final String HEADER_SERVER = "SERVER";
	private static final String HEADER_DATE = "DATE";
	private static final String HEADER_CONTENT = "content-type";

	private static final String SERVER = "vertx";

	private static final String DB_WORLD = "World";
	private static final String DB_FORTUNE = "Fortune";
	private static final String DB_NAME = "hello_world";
	private static final int DB_PORT = 27017;

	private static final String TEMPLATE_FORTUNE = "<!DOCTYPE html><html><head><title>Fortunes</title></head><body><table><tr><th>id</th><th>message</th></tr><#list messages as message><tr><td>${message._id?html}</td><td>${message.message?html}</td></tr></#list></table></body></html>";

	private Template ftlTemplate;

	private MongoClient mongoClient;

	private String dateString;

	private HttpServer server;

	@Override
	public void start() {

		int port = 8080;

		server = vertx.createHttpServer();
      
		JsonObject config = new JsonObject();
		String host = System.getenv("DBHOST");
		if ( host != null && host.length() > 0 ) {
			config.put("host", host);
		} else {
			config.put("host", "localhost");
		}

		config.put("port", DB_PORT).put("db_name", DB_NAME);
		config.put("maxPoolSize", 500).put("minPoolSize", 50).put("waitQueueMultiple", 10000);

		mongoClient = MongoClient.createShared(vertx, config);

		try { 
			ftlTemplate = new Template("Fortune", new StringReader(TEMPLATE_FORTUNE), 
					new Configuration()); 
		} catch (Exception ex) { 
			ex.printStackTrace(); 
		} 

		server.requestHandler(WebServer.this).listen(port);

		dateString = java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now());

		vertx.setPeriodic(1000, handler -> {
			dateString = java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now());
		});
	}

	@Override
	public void handle(HttpServerRequest request) {
		switch (request.path()) {
		case PATH_PLAINTEXT:
			handlePlainText(request);
			break;
		case PATH_JSON:
			handleJson(request);
			break;
		case PATH_DB:
			handleDbMongo(request);
			break;
		case PATH_QUERIES:
			handleDBMongo(request, false);
			break;
		case PATH_UPDATES:
			handleDBMongo(request, true);
			break;
		case PATH_FORTUNES:
			handleFortunes(request);
			break;
		default:
			request.response().setStatusCode(404);
			request.response().end();
		}
	}

	@Override
	public void stop(){
		if ( mongoClient != null ) mongoClient.close();
		if ( server != null ) server.close();        
	}

	private void handlePlainText(HttpServerRequest request) {
		request.response()
		.putHeader(HEADER_CONTENT, RESPONSE_TYPE_PLAIN).putHeader(HEADER_SERVER,  SERVER)
		.putHeader(HEADER_DATE, dateString).end(HELLO_WORLD_BUFFER);
	}

	private void handleJson(HttpServerRequest request) {
		Buffer buff = Buffer.buffer(Json.encode(Collections.singletonMap(TEXT_MESSAGE, HELLO_WORLD)));
		request.response().putHeader(HEADER_CONTENT, RESPONSE_TYPE_JSON).putHeader(HEADER_SERVER,  SERVER)
		.putHeader(HEADER_DATE, dateString).end(buff);
	}
	
	private void handleFortunes(HttpServerRequest request) {
		mongoClient.find(DB_FORTUNE, new JsonObject(), handler -> {

			List<JsonObject> fortunes = handler.result();
			fortunes.add(new JsonObject().put(UNDERSCORE_ID,0).put(TEXT_MESSAGE, ADD_FORTUNE_MESSAGE));

			//Convert JsonObjects to Map for template to work correctly
			List<Map<String, Object>> fortunes2 = fortunes.stream()
					.sorted((f1, f2) -> f1.getString(TEXT_MESSAGE).compareTo(f2.getString(TEXT_MESSAGE)))
					.map( m -> { return m.getMap(); })
					.collect(Collectors.toList());

			Map model = new HashMap();
			model.put(TEXT_MESSAGES, fortunes2);
			Writer writer = new StringWriter();
			try { ftlTemplate.process(model, writer); } catch (Exception ex) { ex.printStackTrace(); }

			request.response().putHeader(HEADER_CONTENT, RESPONSE_TYPE_HTML)
			.putHeader(HEADER_SERVER,  SERVER)
			.putHeader(HEADER_DATE, dateString)
			.end(writer.toString());
		});
	}


	private void handleDbMongo(HttpServerRequest request) {
		mongoClient.findOne(DB_WORLD, new JsonObject().put(UNDERSCORE_ID, (random.nextInt(10000) + 1)), new JsonObject(), handler -> {
			JsonObject world = getResultFromReply(handler);
			sendResponse(request, world.encode());
		});
	}

	private JsonObject getResultFromReply(AsyncResult<JsonObject> reply) {    
		//Move _id to id
		JsonObject body = reply.result();
		body.put(TEXT_ID,  body.getInteger(UNDERSCORE_ID));
		body.remove(UNDERSCORE_ID);
		return body;
	}

	private void handleDBMongo(HttpServerRequest request, boolean updates) {
		int queriesParam = 1;

		try {
			queriesParam = Integer.parseInt(request.params().get(TEXT_QUERIES));
		} catch (NumberFormatException e) {
			queriesParam = 1;
		}

		//Queries must be between 1 and 500
		queriesParam = Math.max(1, Math.min(queriesParam, 500));

		final MongoHandler dbh = new MongoHandler(request, queriesParam, updates);

		IntStream.range(0, queriesParam).forEach(nbr -> { 
			findRandom(dbh); 
		});
	}

	private void findRandom(Handler<AsyncResult<JsonObject>> handler) {
		mongoClient.findOne(DB_WORLD, new JsonObject().put(UNDERSCORE_ID, (random.nextInt(10000) + 1)), new JsonObject(), handler);
	}

	private void updateRandom(int id) {
		JsonObject update = new JsonObject().put("$set", new JsonObject().put(RANDOM_NUMBER, random.nextInt(10000) + 1));
		mongoClient.update(DB_WORLD, new JsonObject().put(UNDERSCORE_ID, id), update, handler-> {
			if ( handler.failed() ) {
				handler.cause().printStackTrace();
			}
		});
	}

	private void sendResponse(HttpServerRequest request, String result) {
		request.response().putHeader(HEADER_CONTENT, RESPONSE_TYPE_JSON)
		.putHeader(HEADER_SERVER,  SERVER)
		.putHeader(HEADER_DATE, dateString)
		.end(result);
	}

	private final class MongoHandler implements Handler<AsyncResult<JsonObject>> {
		private final HttpServerRequest request;
		private final int queries;
		private final JsonArray worlds;
		private final boolean randomUpdates;

		public MongoHandler(HttpServerRequest rq, int queriesParam, boolean performRandomUpdates) {
			request = rq;
			queries = queriesParam;
			randomUpdates = performRandomUpdates;
			worlds = new JsonArray();
		}
		@Override
		public void handle(AsyncResult<JsonObject> reply) {
			JsonObject world = getResultFromReply(reply);       

			worlds.add(world);
			if (worlds.size() == this.queries) {
				// All queries have completed; send the response.
				String result = worlds.encode();
				sendResponse(request, result);
			}

			if (randomUpdates) 
				updateRandom(world.getInteger(TEXT_ID)); 
		}
	}

	public static void main(String[] args) {
		int procs = Runtime.getRuntime().availableProcessors();
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(WebServer.class.getName(), 
				new DeploymentOptions().setInstances(procs*2), event -> {
					if (event.succeeded()) {
						logger.debug("Your Vert.x application is started!");
					} else {
						logger.error("Unable to start your application", event.cause());
					}
				});
	}
}
