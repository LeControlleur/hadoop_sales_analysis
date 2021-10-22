# Analyse des ventes

Ce fichier est à exécuter sur la machine virtuelle

Vous devez suivre les étapes suivantes : 

*   Mettre le fichier à traiter sur HDFS
    
    `hadoop fs -put sales_world_10k.csv /`



*   Exécuter le programme JAVA compilé en fonction de la tâche à exécuter

    -   Obtenir le profit total obtenu par région du monde
        
        `hadoop target/jar sales_analysis-1.0.jar org.mbds.hadoop.SalesAnalysis /newsales_world_10k.csv /results_sales_analysis_regions Region`

        `hadoop fs -cat /results_sales_analysis_regions/*`


    -   Obtenir le profit total obtenu par pays
        
        `hadoop target/jar sales_analysis-1.0.jar org.mbds.hadoop.SalesAnalysis /newsales_world_10k.csv /results_sales_analysis_country Country`

        `hadoop fs -cat /results_sales_analysis_country/*`


    -   Obtenir le profit total obtenu par type d’item
        
        `hadoop target/jar sales_analysis-1.0.jar org.mbds.hadoop.SalesAnalysis /newsales_world_10k.csv /results_sales_analysis_item_type Item\ Type`

        `hadoop fs -cat /results_sales_analysis_item_type/*`


    -   Pour chaque type d’item, obtenir deux valeurs: la quantité de ventes en ligne et La quantité de ventes hors ligne
        
        `hadoop target/jar sales_analysis-1.0.jar org.mbds.hadoop.SalesAnalysis /newsales_world_10k.csv /results_sales_analysis_item_type_by_channel Item\ Type\ by\ channel`

        `hadoop fs -cat /results_sales_analysis_item_type_by_channel/*`
