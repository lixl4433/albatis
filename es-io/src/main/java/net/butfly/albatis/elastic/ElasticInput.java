package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

@Deprecated
public final class ElasticInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap> {
	private static final long serialVersionUID = -8426373976437566637L;
	protected static final Logger logger = Logger.getLogger(ElasticInput.class);
	private final ElasticConnection elastic;
	// view time def 10 minute
	private int scrolltime = 10;
	// scrollnumber
	private int scrollnumber = 100;
	private String index;
	private String type;
	private QueryBuilder query;
	private ConcurrentLinkedQueue<Rmap> datas = new ConcurrentLinkedQueue<Rmap>();
	private AtomicBoolean es_has_data = new AtomicBoolean(true);
	private AtomicBoolean queue_is_not_enough_data = new AtomicBoolean(true);
	private AtomicBoolean is_first = new AtomicBoolean(true);

	public ElasticInput(String name, ElasticConnection conn) throws IOException {
		super(name);
		elastic = conn;
		index = elastic.getDefaultIndex();
		type = elastic.getDefaultType();
		closing(()->{
			if(!es_has_data.get() && datas.size()<=0) {
				conn.close();
				this.closed();
			}
		});
	}

	public ElasticInput(String name, ElasticConnection conn, QueryBuilder query) throws IOException {
		super(name);
		this.query = query;
		elastic = conn;
		index = elastic.getDefaultIndex();
		type = elastic.getDefaultType();
		closing(()->{
			if(!es_has_data.get() && datas.size()<=0) {
				conn.close();
				this.closed();
			}
		});
	}

	public int getScrolltime() {
		return scrolltime;
	}

	public void setScrolltime(int scrolltime) {
		this.scrolltime = scrolltime;
	}

	public int getScrollnumber() {
		return scrollnumber;
	}

	public void setScrollnumber(int scrollnumber) {
		this.scrollnumber = scrollnumber;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}
	public ElasticInput index(String index) {
		this.index = index;
		return this;
	}
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public SearchResponse scanType() {
		if (index == null) throw new RuntimeException("index is null");
		SearchRequestBuilder searchRequest = elastic.client.prepareSearch(index);
		searchRequest.setSize(scrollnumber)
				// .setSearchType(SearchType.SCAN)
				.setScroll(TimeValue.timeValueMinutes(scrolltime));
		if (query != null) searchRequest.setQuery(query);
		else searchRequest.setQuery(QueryBuilders.matchAllQuery());
		logger.trace(searchRequest.toString());
		SearchResponse searchResponse = searchRequest.execute().actionGet();
		return searchResponse;
	}

	private SearchResponse scanType2(String  scrollId) {
		 SearchResponse actionGet = elastic.client.prepareSearchScroll(scrollId).setScroll(TimeValue.timeValueMinutes(scrolltime)).execute()
				.actionGet();
		 return actionGet;
	}

	@Override
	public Rmap dequeue() {
		if(is_first.get()) add();
		if(datas.size() < 30) {
			queue_is_not_enough_data.set(true);
		}
		return datas.poll();
	}
	
	public void add() {
		is_first.set(false);
		new Thread(()->{
			SearchResponse ss = null;
			while(es_has_data.get()) {
				if(queue_is_not_enough_data.get()) {
					if (ss == null) {
						ss = scanType();
					} else {
						String scrollId = ss.getScrollId();
						ss = scanType2(scrollId);
					}
					if(ss.getHits().getHits().length <=0) {
						es_has_data.set(false);
					}
					for (SearchHit at : ss.getHits()) {
						Rmap ramp = new Rmap();
						Map<String, Object> sourceAsMap = at.getSourceAsMap();
						for (Entry<String, Object> kv : sourceAsMap.entrySet()) {
							String key = kv.getKey();
							Object value = kv.getValue();
							if (null == value) {
								continue;
							}
							ramp.putIfAbsent(key, value);
						}
						datas.add(ramp);
					}
					queue_is_not_enough_data.set(true);
				}
			}
		}).start();;
	}
}
