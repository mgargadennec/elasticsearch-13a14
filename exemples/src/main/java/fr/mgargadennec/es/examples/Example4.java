package fr.mgargadennec.es.examples;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.fluttercode.datafactory.impl.DataFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Recherche avec Mapping
 * Cet exemple reprend le fonctionnement de l'exemple 3, mais on charge ici du paramétrage supplémentaire sur
 * l'index que l'on crée. (settings / mapping)
 *
 * Les settings permettent de définir des analyzers custom ainsi que de la configuration de l'index (nombre de shards, replicas, ...)
 * Les mappings permettent d'appliquer pour chaque champ du document une stratégie d'indexation (analyzer à l'indexation, analyzer à la recherche, stockage du champ, etc...)
 *
 * Remarque : un même champ source peut être indexé plusieurs fois de façon différente à l'aide des mappings.
 *
 * @author mgargadennec
 *
 */
public class Example4 
{
    public static void main( String[] args ) throws InterruptedException, IOException
    {

    	//Création d'un noeud et récupération d'un client
    	Node node = NodeBuilder.nodeBuilder().clusterName("exemple4").local(true).node();
    	Client client = node.client();

		//On attend le statut "jaune" (= index rechargés)
		client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();

    	//Paramétrage
    	String indexName = "mon_index";
    	String type = "mon_type";

		//Initialisation de l'index
    	initIndex(client, indexName, type);

        Scanner keyboard = new Scanner(System.in);
        String queryString = null;
        do{
        	System.out.println("Enter a search term :");
            queryString = keyboard.nextLine();	
            
            //Une réponse
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(
            		QueryBuilders.functionScoreQuery(QueryBuilders.queryStringQuery(queryString)
                    		.field("titre",10)
                    		.field("sousTitre"))
            		).get();
            System.out.println(searchResponse.toString());
            
        }while(!queryString.equals("exit"));
        keyboard.close();
        
    	node.close();
    	
    	System.exit(0);
    }

	private static void initIndex(Client client, String indexName, String type) throws InterruptedException, IOException {
		//Nettoyage des données précédentes
    	DeleteIndexResponse deleteResponse = client.admin().indices().prepareDelete("_all").get();
    	System.out.println("Is deleted : "+deleteResponse.isAcknowledged());
    	 
    	//Datafactory
    	DataFactory df = new DataFactory();

		//Chargement d'un fichier JSON de mapping à l'aide de Guava (peut-être buildé manuellement à l'aide des XContentBuilder également)
    	URL url = Resources.getResource("mapping.json");
    	String mapping = Resources.toString(url, Charsets.UTF_8);
    	
    	System.out.println(mapping);

		//Chargement d'un fichier JSON de settings à l'aide de Guava (peut-être buildé manuellement à l'aide des XContentBuilder également)
    	URL urlSettings = Resources.getResource("settings.json");
    	String settings = Resources.toString(urlSettings, Charsets.UTF_8);
    	System.out.println(settings);
    	
    	//Création de l'index avec mapping
    	client
    		.admin()
    		.indices()
    		.prepareCreate(indexName)
    		.setSettings(settings) //Application des settings sur l'index
    		.addMapping(type, mapping) // Application des mappings sur le type voulu (peut-etre ajouté après création)
    			.execute().actionGet();

    	//Un objet à indexer
    	Map<String,Object> object = Maps.newHashMap();
    	object.put("id",UUID.randomUUID().toString());
    	object.put("titre","Ma première indexation est un succès!");
    	object.put("sousTitre","En espérant qu'aucune erreur ne vienne poser problème :'( ");
    	object.put("year",2015);
    	object.put("createdAt",DateTime.now());
    	
    	//Indexation unitaire
    	IndexResponse response = client.prepareIndex(indexName, type).setSource(object).execute().actionGet();
        System.out.println("First content indexed with id: " + response.getId()+" and type "+response.getType());

    	//Indexation en bulk
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        
        for(int i=0;i<5999;i++){
        	StringBuilder b = new StringBuilder();
			do{
				b.append(df.getRandomWord(2,10));
				b.append(" ");
			}while(b.length()<70);

			//On manipule dans l'objet différents types
			Map<String,Object> item = Maps.newHashMap();
			item.put("id",UUID.randomUUID().toString()); //ID : ne doit pas être analysé
			item.put("titre",b.toString()); //Titre : analysé avec l'analyzer "french"
			item.put("sousTitre",b.toString()); // Sous-Titre : analyzer custom
			item.put("year",df.getNumberBetween(1950, 2015)); // Année : entier aléatoire
			item.put("createdAt",df.getDateBetween(DateTime.now().minusYears(3).toDate(), DateTime.now().toDate())); // ES gère très bien les dates (merci JodaTime)

        	bulkRequest.add(client.prepareIndex(indexName,type).setSource(item));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        System.out.println(bulkResponse.getItems().length+" documents indexed in "+bulkResponse.getTookInMillis()+"ms. Any failures ? "+bulkResponse.hasFailures() );
    	        
        Thread.sleep(2000);

        //On compte le nombre total de documents dans l'index
        CountResponse countResponse2 = client.prepareCount(indexName).get();
        System.out.println("Total documents "+countResponse2.getCount());
		
	}
}
