package datawave.formatters;

import datawave.query.util.FrequencyFamilyCounter;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class FrequencyColumnValueFormatter implements Formatter {
    
    private Iterator<Map.Entry<Key,Value>> iter;
    private FormatterConfig config;
    private FrequencyFamilyCounter frequencyFamilyCounter = new FrequencyFamilyCounter();
    
    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }
    
    @Override
    public String next() {
        StringBuilder sb = new StringBuilder();
        
        Map.Entry<Key,Value> entry = iter.next();
        
        if (entry.getKey().getColumnQualifier().toString().startsWith(MetadataHelper.COL_QUAL_PREFIX)) {
            frequencyFamilyCounter.deserializeCompressedValue(entry.getValue());
            frequencyFamilyCounter.getDateToFrequencyValueMap().entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
                            .forEach(sorted -> sb.append("Date: " + sorted.getKey() + " Frequency: " + sorted.getValue().toString() + "\n"));
            return sb.toString();
        } else {
            return DefaultFormatter.formatEntry(entry, false);
        }
        
    }
    
    @Override
    public void remove() {
        iter.remove();
    }
    
    @Override
    public void initialize(Iterable<Map.Entry<Key,Value>> scanner, FormatterConfig formatterConfig) {
        this.iter = scanner.iterator();
    }
    
}
