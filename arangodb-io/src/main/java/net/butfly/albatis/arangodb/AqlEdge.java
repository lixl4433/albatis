package net.butfly.albatis.arangodb;

import java.util.Map;

import net.butfly.albatis.io.Rmap;

//[{"_key":"1","_id":"friends/1","_from":"people/1","_to":"people/2","_rev":"_bxgJIFi---","rel":"朋友"}]
public class AqlEdge extends Rmap{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Map<String, Object> from;
	private Map<String, Object> to;
	private Map<String, Object> edge;
	
	
	public Map<String, Object> getFrom() {
		return from;
	}
	public void setFrom(Map<String, Object> from) {
		this.from = from;
	}
	public Map<String, Object> getTo() {
		return to;
	}
	public void setTo(Map<String, Object> to) {
		this.to = to;
	}
	public Map<String, Object> getEdge() {
		return edge;
	}
	public void setEdge(Map<String, Object> edge) {
		this.edge = edge;
	}
}
