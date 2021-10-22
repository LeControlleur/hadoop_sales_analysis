/*
  M2 MBDS - Big Data/Hadoop
	Année 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountReduce.java: classe REDUCE.
*/
package org.mbds.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;

// Notre classe REDUCE - templatée avec un type générique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class SalesAnalysisReduce extends Reducer<Text, Text, Text, Text> {
	String[] valueArray;
	Boolean is_question_4 = false;
	int item_index;

	// La fonction REDUCE elle-même. Les arguments: la clef key (d'un type générique
	// K), un Iterable de toutes les valeurs
	// qui sont associées à la clef en question, et le contexte Hadoop (un handle
	// qui nous permet de renvoyer le résultat à Hadoop).
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Pour parcourir toutes les valeurs associées à la clef fournie.
		Iterator<Text> i = values.iterator();
		float totalQte = 0;
		float totalProfit = 0;

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

		if (is_question_4) {
			while (i.hasNext()) {
				// Notre valeur contient 2 éléments
				// [2] : quantité de vente => index 8
				// [2] : profit de vente => index -1
				valueArray = i.next().toString().replace("[", "").replace("]", "").split(", ");

				totalQte += Float.parseFloat(valueArray[0]);
				totalProfit += Float.parseFloat(valueArray[1]);
			}
			context.write(key, new Text("Total quantité : " + totalQte + "; Total profit : " + totalProfit));
		} else {
			while (i.hasNext()) {
				// Notre valeur contient 2 éléments
				// [2] : quantité de vente => index 8
				// [2] : profit de vente => index -1
				totalProfit += Float.parseFloat(i.next().toString());
			}
			context.write(key, new Text("Total profit : " + totalProfit));
		}
	}
}
