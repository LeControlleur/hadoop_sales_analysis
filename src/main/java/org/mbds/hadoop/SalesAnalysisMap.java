/*
  M2 MBDS - Big Data/Hadoop
	Année 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountMap.java: classe MAP.
*/
package org.mbds.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;


// Notre classe MAP.
public class SalesAnalysisMap extends Mapper<Object, Text, Text, Text>
{
	Configuration conf;
	int item_index;
	String[] line;
	float[] new_value = new float[2];
	Boolean is_question_4 = false;

	// La fonction MAP elle-même.
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{  
		//	On fait un split pour séparer les données de notre ligne csv
		line = value.toString().split(",");

		//	Parcours des données et récupération des données qui nous intéresse en fonction de l'argument passé en paramètre

		switch (context.getConfiguration().get("use_case")) {
			case "1":
				item_index = 0;
				break;
			
			case "2":
				item_index = 1;
				break;

			case "3":
				item_index = 2;
				break;

			case "4":
				is_question_4 = true;
				item_index = 2;
				break;
		
			default:
				return;
		}

		//	On renvoie notre couple clé valeur
		if(is_question_4) {
			//	Notre valeur contient 2 éléments
			//	[2] : quantité de vente			=> index 8
			//	[2] : profit de vente			=> index -1

			new_value[0] = Float.parseFloat(line[8]);
			new_value[1] = Float.parseFloat(line[line.length - 1]);
			context.write(new Text(line[item_index] + " - " + line[3]), new Text(Arrays.toString(new_value)));
		} 
		else {
			context.write(new Text(line[item_index]), new Text(line[line.length - 1]));
		}
	}
}
