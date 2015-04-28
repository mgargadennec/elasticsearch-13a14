package fr.mgargadennec.es.examples;

import java.util.UUID;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Création d'index
 * Création d'un noeud embarqué, récupération d'un client, création d'un index, lecture de la liste des index disponibles
 * 
 * @author mgargadennec
 */
public class Example1 
{
    public static void main( String[] args ){
    	//Création d'un noeud et récupération d'un client
    	Node node = NodeBuilder.nodeBuilder().clusterName("exemple1").local(true).node();
    	Client client = node.client();
    	
    	//On créé un index
    	client.admin().indices().prepareCreate(UUID.randomUUID().toString()).addAlias(new Alias("foobar")).execute().actionGet();
    	    	
    	//On affiche les index existants
    	GetIndexResponse indices = client.admin().indices().prepareGetIndex().execute().actionGet();

    	for(String index: indices.getIndices()){
    		System.out.println("Found index : "+index);
    	}
    	
    	node.close();
    	
    	System.exit(0);
    }
}
