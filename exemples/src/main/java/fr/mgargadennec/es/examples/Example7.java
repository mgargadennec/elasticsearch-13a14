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
import org.elasticsearch.common.base.Strings;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.fluttercode.datafactory.impl.DataFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

/**
 * Recherche : Aggrégations
 * C'est le moment de faire des tentatives de AggregationBuilders !
 *
 * Objectif : calculer des statistiques à plusieurs niveaux dans le contexte de recherche
 *
 * @author mgargadennec
 *
 */
public class Example7 
{
    public static void main( String[] args ) throws InterruptedException, IOException
    {

    	//Création d'un noeud et récupération d'un client
    	Node node = NodeBuilder.nodeBuilder().clusterName("exemple7").node();
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
            
            try{
            SearchResponse searchResponse = client.prepareSearch(indexName)
					// Pendant que la QueryBuilder nous permet d'effectuer une recherche... (ou un matchAll si saisie vide)
					.setQuery(
							Strings.isNullOrEmpty(queryString) ?
									QueryBuilders.matchAllQuery()
									:
									QueryBuilders.simpleQueryStringQuery(queryString)

					//L'aggrégation permet de calculer des statistiques sur les documents matchés par la recherche

					//Ici on prend les 25 dernières années par ordre décroissant
					).addAggregation(
							AggregationBuilders
									.terms("byYear")
									.field("year")
									.size(25)
									.order(Terms.Order.term(false))
									.subAggregation(
											//Et pour chacune, on calcule les 5 catégories contenant le plus de documents
											AggregationBuilders
													.terms("byCategory")
													.field("category")
            										.size(5)
								)
							)
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
    	object.put("category", "Personnalisé");
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
	    	item.put("category", df.getBusinessName());
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
