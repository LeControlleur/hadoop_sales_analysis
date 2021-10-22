/*
  M2 MBDS - Big Data/Hadoop
	Année 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  WCountMap.java: classe driver (contient le main du programme).
*/
package org.mbds.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;



// Note classe Driver (contient le main du programme Hadoop).
public class SalesAnalysis
{

	// Le main du programme.
	public static void main(String[] args) throws Exception
	{
		// Créé un object de configuration Hadoop.
		Configuration conf=new Configuration();
		// Permet à Hadoop de lire ses arguments génériques, récupère les arguments restants dans ourArgs.
		String[] ourArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		// Obtient un nouvel objet Job: une tâche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
		// textuelle de la tâche.
		int use_case;
		switch (args[2]) {
			case "Region":
				use_case = 1;
				break;
			
			case "Country":
				use_case = 2;
				break;

			case "Sales channel":
				use_case = 3;
				break;

			case "Item Type by channel":
				use_case = 4;
				break;
		
			default:
				System.exit(-1);
				return;
		}
		conf.set("use_case", ""+use_case);

		
		Job job=Job.getInstance(conf, "Analyse des données de vente");

		// Défini les classes driver, map et reduce.
		job.setJarByClass(SalesAnalysis.class);
		job.setMapperClass(SalesAnalysisMap.class);
		job.setReducerClass(SalesAnalysisReduce.class);

		// Défini types clefs/valeurs de notre programme Hadoop.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Défini les fichiers d'entrée du programme et le répertoire des résultats.
		// On se sert du premier et du deuxième argument restants pour permettre à l'utilisateur de les spécifier
		// lors de l'exécution.
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));

		// On lance la tâche Hadoop. Si elle s'est effectuée correctement, on renvoie 0. Sinon, on renvoie -1.
		if(job.waitForCompletion(true))
			System.exit(0);
		System.exit(-1);
	}
}
