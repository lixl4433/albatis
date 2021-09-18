package net.butfly.albatis.arangodb;

//[{"_key":"1","_id":"friends/1","_from":"people/1","_to":"people/2","_rev":"_bxgJIFi---","rel":"朋友"}]
public class AqlEdge{
	private ArangoDBNode from;
	private ArangoDBNode to;
	private ArangoDBEdge edge;
	
	
	public ArangoDBNode getFrom() {
		return from;
	}
	public void setFrom(ArangoDBNode from) {
		this.from = from;
	}
	public ArangoDBNode getTo() {
		return to;
	}
	public void setTo(ArangoDBNode to) {
		this.to = to;
	}
	public ArangoDBEdge getEdge() {
		return edge;
	}
	public void setEdge(ArangoDBEdge edge) {
		this.edge = edge;
	}
}
