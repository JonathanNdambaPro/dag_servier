# Dag Servier

Ce projet consiste en un DAG (Directed Acyclic Graph, ou graphe acyclique orienté) ayant pour but l'importation de données dans Google Cloud Storage (GCS).

## Environnement Virtuel 
Nous utilisons `pip` pour une gestion plus aisée des packages personnalisés au sein d'un environnement virtuel.

### Création d'un Environnement Virtuel
Pour créer un environnement virtuel dans le répertoire courant, exécutez :

```bash
python -m venv .
```

### Activation d'environnement virtuel

Activez l'environnement virtuel avec la commande suivante :
```bash
source bin/activate
```

### Installation des Dépendances

Installez les dépendances requises en exécutant :

```bash
pip install -r requirements.txt
```

### Installation du package personnalisé
Ce package constitue le cœur du pipeline et contient toute la logique nécessaire. Pour plus de détails, vous pouvez :

- [Voir le répo](https://github.com/JonathanNdambaPro/servier)
- [Voir la doc](https://jonathanndambapro.github.io/servier/docs/app.html)

Pour installer le package personnalisé, utilisez la commande suivante :

```bash
pip install dist/app-0.1.0-py3-none-any.whl 
```



