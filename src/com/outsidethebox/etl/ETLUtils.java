package com.outsidethebox.etl;

class ETLUtils {
	public static String[] getProperty(String part) throws StringIndexOutOfBoundsException{
		if(part==null){
			return null;
		}
		if (part.matches("\\{property\\((([a-zA-Z0-9])*),(([a-zA-Z0-9\\._])*)\\)\\}")) {
			int indxProp = part.indexOf("{property(");
			int indxProp10 = indxProp + 10;
			int indxComma = part.indexOf(",");
			int indxComma1 = indxComma + 1;
			int indxEnd = part.indexOf(")}");
			if (indxProp >= 0 && indxProp10 >= 0 && indxComma >= 0 && indxComma1 >= 0 && indxEnd >= 0) {
				String propId = part.substring(indxProp10, indxComma);
				String propKey = part.substring(indxComma1, indxEnd);
				return new String[] { propId, propKey };
			}
		}
		return null;
	}
}
