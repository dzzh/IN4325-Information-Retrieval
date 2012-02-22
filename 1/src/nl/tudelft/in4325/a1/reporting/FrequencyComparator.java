package nl.tudelft.in4325.a1.reporting;

import nl.tudelft.in4325.a1.Constants;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

public class FrequencyComparator implements RawComparator<Text>{

	private static final char separator = Constants.FIELD_SEPARATOR.charAt(0);
	
	@Override
	public int compare(Text arg0, Text arg1) {
		return compare(arg0.getBytes(), 0, arg0.getLength() - 1, arg1.getBytes(), 0, arg1.getLength() - 1);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		
		int result =extractFrequency(b2, s2, l2) - extractFrequency(b1, s1, l1);
		
		if (result != 0){
			return result;
			
		} else{

			int offset = 0;
			int minLength = Math.min(l1, l2);
			
			while (offset < minLength){
				char c1 = (char)b1[s1 + 1 + offset];
				char c2 = (char)b2[s2 + 1 + offset];
				offset++;
				
				if (c1 != c2){
					return c2 - c1;
				} 
			}
			
			return l2 - l1;
		}
	}

	private int extractFrequency(byte[] b, int s, int l){
		
		boolean hasSeparator = false;
		int i = 0;
				
		while (i < l){
			char c = (char)b[s + 1 + i];
			if (c == separator){
				hasSeparator = true;
				break;
			} else if (!Character.isDigit(c)){
				return 0;
			}
			i++;
		}
		
		if (!hasSeparator){
			return 0;
		} else {
			StringBuffer sb = new StringBuffer();
			for (int j = 0; j < i; j++){
				sb.append((char)b[s + 1 + j]);
			}
			if (sb.length() != 0){
				int result = Integer.valueOf(sb.toString());
				return result;
			} else{
				return 0;
			}
		}
		
	}
	
}
