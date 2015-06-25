package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;

public class InternalPathHierarchy extends InternalAggregation implements PathHierarchy {

    public static final Type TYPE = new Type("path_hierarchy", "phierarchy");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPathHierarchy readResult(StreamInput in) throws IOException {
            InternalPathHierarchy buckets = new InternalPathHierarchy();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Bucket implements PathHierarchy.Bucket {

        BytesRef termBytes;

        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected List<Bucket> children;

        public Bucket(BytesRef term, long docCount, InternalAggregations aggregations, int level) {
            this.termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.children = new ArrayList<>();
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        public Text getKeyAsText() {
            return new BytesText(new BytesArray(termBytes));
        }

        @Override
        public int compareTerm(PathHierarchy.Bucket other) {
            return BytesRef.getUTF8SortedAsUnicodeComparator().compare(termBytes, ((Bucket) other).termBytes);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext reduceContext) {
            List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>(buckets.size());
            Bucket reduced = null;
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            return reduced;
        }

    }

    private List<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    private InternalOrder order;
    private String separator;

    InternalPathHierarchy() {
    } // for serialization

    public InternalPathHierarchy(String name, List<Bucket> buckets, InternalOrder order, String separator) {
        super(name);
        this.buckets = buckets;
        this.order = order;
        this.separator = separator;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PathHierarchy.Bucket> getBuckets() {
        Object o = buckets;
        return (List<PathHierarchy.Bucket>) o;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PathHierarchy.Bucket getBucketByKey(String path) {
      if (bucketMap == null) {
          bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
          for (Bucket bucket : buckets) {
              bucketMap.put(bucket.getKey(), bucket);
          }
      }
      return bucketMap.get(path);
    }

    @Override
    public InternalPathHierarchy reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();

        HashMultimap<BytesRef, Bucket> buckets = HashMultimap.create ();
        for (InternalAggregation aggregation : aggregations) {
            InternalPathHierarchy pathHierarchy = (InternalPathHierarchy) aggregation;

            for (Bucket bucket : pathHierarchy.buckets) {
              buckets.put (bucket.termBytes, bucket);
            }
            
        }

        List<Bucket> reduced = new ArrayList<Bucket>((int) buckets.size());

        for (Entry<BytesRef, Collection<Bucket>> entry : buckets.asMap ().entrySet ()) {
            List<Bucket> sameCellBuckets = new ArrayList<Bucket> (entry.getValue ());
            reduced.add(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
        }

        CollectionUtil.introSort(reduced, order.comparator());

        return new InternalPathHierarchy(getName(), reduced, order, separator);
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        int listSize = in.readVInt();
        this.buckets = new ArrayList<InternalPathHierarchy.Bucket> (listSize);
        for (int j=0; j < listSize; j++) {
          Bucket bucket = new Bucket(in.readBytesRef(), in.readLong(), InternalAggregations.readAggregations(in), in.readInt());
          this.buckets.add (bucket);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVInt(buckets.size());
        for (Bucket bucket: buckets) {
          out.writeBytesRef(bucket.termBytes);
          out.writeLong(bucket.docCount);
          ((InternalAggregations) bucket.getAggregations()).writeTo(out);
          out.writeInt(bucket.level);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS);

        for (Bucket bucket : this.buckets) {
          builder.startObject();
          builder.field(CommonFields.KEY, bucket.termBytes.utf8ToString ());
          builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
          ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
          builder.endObject();
        }
        
        builder.endArray();
        return builder;
    }

}
