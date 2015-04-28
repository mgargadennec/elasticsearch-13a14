package fr.mgargadennec.es.examples;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;

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
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.fluttercode.datafactory.impl.DataFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Recherche : Filters
 * C'est le moment de faire des tentatives de FilterBuilders !
 *
 * Objectif : Restreindre le périmètre de recherche sur de l'exclusion/inclusion de documents, basés sur des valeurs exactes ou des ranges de valeurs
 * 
 * @author mgargadennec
 *
 */
public class Example6 
{
    public static void main( String[] args ) throws InterruptedException, IOException
    {

    	//Création d'un noeud et récupération d'un client
    	Node node = NodeBuilder.nodeBuilder().clusterName("exemple6").node();
    	Client client = node.client();

		//On attend le statut "jaune" (= index rechargés)
		client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();

    	//Paramétrage
    	String indexName = "mon_index";
    	String type = "mon_type";
    	
    	initIndex(client,indexName,type);
    	

        Scanner keyboard = new Scanner(System.in);
        String queryString = null;
        do{
        	System.out.println("Enter a search term :");
            queryString = keyboard.nextLine();	

        	System.out.println("Enter a year to filter on :");
            String filter = keyboard.nextLine();	
            
            try{
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(
					// La filteredQuery permet d'appliquer un filtre en amont de la recherche
					// Ce filtre peut-etre un booleanFilter, contenant des andFilter/orFilter, etc...
					// Composez un ensemble de filtres selon vos besoins !
            		QueryBuilders.filteredQuery(
            				QueryBuilders.simpleQueryStringQuery(queryString), 
            				FilterBuilders.termFilter("year", Integer.parseInt(filter)))
            		)

					//D'autres filtres existent (postfilter) mais ne sont pas conseillés dans la plupart des cas,
					// cas ils s'executent en dernier (donc après l'ensemble des calculs, meme sur les documents
					// finalement exclus)
					//.setPostFilter(FilterBuilders.termFilter("year", Integer.parseInt(filter)))

					.get();
            System.out.println(searchResponse.toString());
            }catch(Exception e){
            	
            }
        }while(!queryString.equals("exit"));
        keyboard.close();
        
    	node.close();
    	
    	System.exit(0);
    }

	private static void initIndex(Client client, String indexName, String type) throws InterruptedException, IOException {
		//Nettoyage des données précédentes
    	client.admin().indices().prepareDelete("_all").execute().actionGet();
    	
    	//Datafactory
    	DataFactory df = new DataFactory();


    	//Mapping
    	URL url = Resources.getResource("mapping.json");
    	String mapping = Resources.toString(url, Charsets.UTF_8);
    	
    	//Settings
    	URL urlSettings = Resources.getResource("settings.json");
    	String settings = Resources.toString(urlSettings, Charsets.UTF_8);
    	
    	//Création de l'index avec mapping
    	client.admin().indices().prepareCreate(indexName).setSettings(settings).addMapping(type, mapping).execute().actionGet();

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
			
			Map<String,Object> item = Maps.newHashMap();
			item.put("id",UUID.randomUUID().toString());
			item.put("titre",b.toString());
			item.put("sousTitre",b.toString());
			item.put("year",df.getNumberBetween(1950, 2015));
			item.put("createdAt",df.getDateBetween(DateTime.now().minusYears(3).toDate(), DateTime.now().toDate()));
				
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
