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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.fluttercode.datafactory.impl.DataFactory;

/**
 * Indexation + Indexation Bulk
 *
 * Quelques exemples des possibilités d'indexation de manière unitaire ou en bulk (= en masse)
 * Je vous invite à regarder les méthodes setSource() disponibles sur le builder, afin de voir les possibilités offertes (map/json/ byte[], ...).
 * 
 * @author mgargadennec
 *
 */
public class Example2 
{
    public static void main( String[] args ) throws InterruptedException
    {

    	//Création d'un noeud et récupération d'un client
    	Node node = NodeBuilder.nodeBuilder().clusterName("exemple2").node();
    	Client client = node.client();

        //On attend le statut "jaune" (= index rechargés)
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();

    	//Nettoyage des données précédentes (_all indique que tous les index doivent être supprimés)
    	client.admin().indices().prepareDelete("_all").execute().actionGet();
    	
    	//Datafactory = générateur de données aléatoires
    	DataFactory df = new DataFactory();

    	//Paramétrage
    	String indexName = "mon_index";
    	String type = "mon_type";
    	
    	//Création de l'index
    	client.admin().indices().prepareCreate(indexName).execute().actionGet();

    	//Un objet basique à indexer
    	Map<String,Object> object = Maps.newHashMap();
    	object.put("titre","Ma première indexation est un succès!");
    	object.put("sousTitre","En espérant qu'aucune erreur ne vienne poser problème :'( ");
    	
    	//Indexation unitaire de l'objet
    	IndexResponse response = client.prepareIndex(indexName, type).setSource(object).execute().actionGet();
        System.out.println("First content indexed with id: " + response.getId()+" and type "+response.getType());

    	//Indexation en bulk
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        //Pour 5431 éléments
        for(int i=0;i<5341;i++){
        	StringBuilder b = new StringBuilder();
			do{
				b.append(df.getRandomWord(2,10));
				b.append(" ");
			}while(b.length()<70);

            //On prend un titre aléatoire
			Map<String,Object> item = Maps.newHashMap();
			item.put("titre",b.toString());

            //Et on ajoute la requête préparée au bulk
        	bulkRequest.add(client.prepareIndex(indexName,type).setSource(item));
        }
        //Execution du bulk
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        //Affichage de la réponse
        System.out.println(bulkResponse.getItems().length+" documents indexed in "+bulkResponse.getTookInMillis()+"ms. Any failures ? "+bulkResponse.hasFailures() );
    	        
        Thread.sleep(2000);

        //On compte le nombre total de documents dans l'index
        CountResponse countResponse2 = client.prepareCount(indexName).get();
        System.out.println("Total documents "+countResponse2.getCount());
        
        
    	node.close();
    	
    	System.exit(0);
    }

}
