package it.polito.tdp.crimes.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.graph.SimpleWeightedGraph;

import it.polito.tdp.crimes.db.EventsDao;

public class Model {
	
	EventsDao dao;
	private SimpleWeightedGraph<District, DefaultWeightedEdge> grafo; 
	Map<Integer , District> idMap;
	public  Model()
	{
		dao= new EventsDao();
	}
	public String creaGrafo(int anno) {
		grafo = new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
		idMap=new HashMap<Integer, District>();
		dao.getDistricts(idMap,anno);
		Graphs.addAllVertices(grafo, idMap.values());
		String s="";
		s+="Vertici "+grafo.vertexSet().size();
		for(District d1: grafo.vertexSet())
		{
			for(District d2:grafo.vertexSet())
			{
				if(!d1.equals(d2))
				{
					if(grafo.getEdge(d2, d1)==null&&grafo.getEdge(d1, d2)==null)
					{
						
						Graphs.addEdge(grafo, d1, d2, Math.sqrt(Math.pow((d1.getGeolat()-d2.getGeolat()),2)+Math.pow((d1.getGeolon()-d2.getGeolon()),2)));
					}
				}
			}
		}
		s+=" Archi: "+grafo.edgeSet().size()+"\n";
		
		for(District d: grafo.vertexSet())
		{
			LinkedList<District> adiacenti= new LinkedList<>(Graphs.neighborListOf(grafo, d));
			Collections.sort(adiacenti,(d1,d2)->(int)(grafo.getEdgeWeight(grafo.getEdge(d, d1))-grafo.getEdgeWeight(grafo.getEdge(d, d2))));
			s+="Adiacenti a distretto "+d.districtid+ "\n";
			for(District d3: adiacenti)
			{
				s+=d3.districtid+" "+(grafo.getEdgeWeight(grafo.getEdge(d, d3)))+"\n";
			}
		}
		return s;
	}
	
	public List<Integer> mesi(int year)
	{
		return dao.getMesi(year);
	}

	public List<Integer> giorni(int year, int month)
	{
		return dao.getGiorni(year, month);
	}
	
	int n=0;
	
	public void init(int n)
	{
		this.n=n;
		
	}
}
