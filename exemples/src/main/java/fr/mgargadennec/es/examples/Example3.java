package fr.mgargadennec.es.examples;

import java.util.Map;
import java.util.Scanner;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.fluttercode.datafactory.impl.DataFactory;

/**
 * Recherche sans mapping
 * Après l'indexation de 6000 documents, on effectue dans cette exemple une recherche "basique" (queryStringQuery)
 * Vous noterez que la recherche des mots accentués du premier exemple ne fonctionne pas : nous verrons comment corriger cela dans l'exemple 4 !
 * 
 * @author mgargadennec
 *
 */
public class Example3 
{
    public static void main( String[] args ) throws InterruptedException
    {

    	//Création d'un noeud et récupération d'un client
    	Node node = NodeBuilder.nodeBuilder().clusterName("exemple3").local(true).node();
    	Client client = node.client();

		//On attend le statut "jaune" (= index rechargés)
		client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();

    	//Nettoyage des données précédentes
    	client.admin().indices().prepareDelete("_all").execute().actionGet();
    	
    	//Datafactory
    	DataFactory df = new DataFactory();

    	//Paramétrage
    	String indexName = "mon_index";
    	String type = "mon_type";
    	
    	//Création de l'index
    	client.admin().indices().prepareCreate(indexName).execute().actionGet();

    	//Un objet à indexer
    	Map<String,Object> object = Maps.newHashMap();
    	object.put("titre","Ma première indexation est un succès!");
    	object.put("sousTitre","En espérant qu'aucune erreur ne vienne poser problème :'( ");
    	
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
			item.put("titre",b.toString());
				
        	bulkRequest.add(client.prepareIndex(indexName,type).setSource(item));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        System.out.println(bulkResponse.getItems().length+" documents indexed in "+bulkResponse.getTookInMillis()+"ms. Any failures ? "+bulkResponse.hasFailures() );
    	        
        Thread.sleep(2000);

        //On compte le nombre total de documents dans l'index
        CountResponse countResponse2 = client.prepareCount(indexName).get();
        System.out.println("Total documents "+countResponse2.getCount());
        
        
        let_start_playing(client,indexName);
        
        
    	node.close();
    	
    	System.exit(0);
    }

	private static void let_start_playing(Client client, String indexName) {

        Scanner keyboard = new Scanner(System.in);
        String queryString = null;
        do{
        	System.out.println("Enter a search term :");
            queryString = keyboard.nextLine();	
            
            //Une réponse
            SearchResponse searchResponse = client.prepareSearch(indexName).setQuery(QueryBuilders.queryStringQuery(queryString)).get();
            System.out.println(searchResponse.toString());
            
        }while(!queryString.equals("exit"));
        keyboard.close();
        
	}
}
