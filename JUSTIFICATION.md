# Justification des Choix Techniques

## 1. Langage de Programmation : Python
Nous avons choisi **Python** pour ce projet en raison de sa dominance dans le domaine de la Data Science et de la BI.
- **Avantages :** Écosystème riche (Pandas, Numpy, Matplotlib), facilité de développement, et capacité à gérer tout le pipeline (ETL + Web App) dans un seul langage.

## 2. Architecture ETL
L'ETL est conçu de manière modulaire :
- **Extract :** Abstraction de la source de données (SQL, Access ou CSV). Sur cet environnement Linux, nous avons privilégié le **CSV Fallback** car les pilotes ODBC Access ne sont pas natifs.
- **Transform :** Utilisation de **Pandas** pour le nettoyage (gestion des dates, types numériques, normalisation des noms de colonnes).
- **Load :** Stockage des données nettoyées en **CSV** (`data/clean`) pour une consommation facile par le dashboard. Architecture "File-based" simple et efficace pour ce volume de données.

## 3. Visualisation : Streamlit
Pour le tableau de bord, nous avons choisi **Streamlit** plutôt qu'un simple notebook Jupyter ou PowerBI desktop (incompatible Linux/Cloud).
- **Interactivité :** Filtres dynamiques (Année).
- **Rapidité :** Permet de transformer des scripts de données en application web en quelques minutes.
- **Esthétique :** Les composants natifs offrent un rendu professionnel immédiat.

## 4. Bibliothèques
- **Pandas :** Manipulation de données structurées.
- **Matplotlib / Seaborn :** Graphiques statiques précis intégrés dans Streamlit.
- **Logging :** Suivi de l'exécution de l'ETL pour le débogage.
