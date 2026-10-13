# Projet_PLE
L’objectif de ce projet est de concevoir un système d’analyse de la distribution statistique des match- ups dans le jeu vidéo Clash Royale. Un match-up correspond au choix de l’adversaire effectué par le système. 


# Partie I — Nettoyage des données (Hadoop MapReduce)

## Hypothèse sur le schéma d'entrée

En l'absence d'un exemple de ligne réelle, le code suppose le schéma suivant
(un objet JSON par ligne) :

```json
{
  "timestamp": 1699999999,
  "round": 12345,
  "playerA": {
    "id": "P123",
    "clan": "C001",
    "level": 12,
    "avgCardLevel": 11.2,
    "deck": ["0001","0006","0007","000a","000f","0003","0004","0002"],
    "win": true
  },
  "playerB": { "id": "P123",
    "clan": "C001",
    "level": 12,
    "avgCardLevel": 11.2,
    "deck": ["0001","0006","0007","000a","000f","0003","0004","0002"],
    "win": true
    }
}
```

**Si votre schéma réel diffère** (noms de champs, structure imbriquée ou à
plat, format du timestamp en millisecondes, etc.), il suffit d'adapter les
noms de clés dans `CleaningMapper.hasValidDeck()` et `map()` — la logique
de validation/dédoublonnage reste inchangée.

## Ce que fait le job

Un seul job MapReduce (Mapper + Reducer) :

1. **Mapper (`CleaningMapper`)**
   - Rejette les lignes qui ne sont pas un JSON valide (`INVALID_JSON`).
   - Rejette les lignes avec des champs manquants (`MISSING_FIELDS`,
     `MALFORMED_FIELDS`, `MISSING_PLAYER_ID`).
   - Rejette les parties où un deck n'a pas exactement 8 cartes
     (`INVALID_DECK_SIZE`).
   - **Normalise l'ordre des joueurs** : les deux joueurs sont réordonnés
     par leur `id` (ordre lexicographique), de sorte qu'une même partie
     décrite "du point de vue de A" ou "du point de vue de B" produise
     exactement le même enregistrement normalisé.
   - Construit une **clé de dédoublonnage logique** :
     `id_joueur_min | id_joueur_max | jour | round`
     où `jour = timestamp / 86400` (division entière). Cela regroupe deux
     enregistrements de la même partie même si leurs timestamps diffèrent
     de quelques secondes, sans fusionner deux parties distinctes jouées
     à des jours différents.

2. **Reducer (`CleaningReducer`)**
   - Toutes les valeurs reçues sous une même clé correspondent à la même
     partie réelle (doublon exact, ou même partie vue par les deux
     joueurs). Le reducer élimine les doublons et n'émet qu'un seul
     enregistrement représentatif par clé.
   - Compteurs `DUPLICATES_REMOVED` et `AMBIGUOUS_DUPLICATE_KEY` (ce
     dernier signale le cas anormal où deux enregistrements partagent la
     même clé logique mais diffèrent sur autre chose que le timestamp —
     utile pour votre analyse qualité en Partie IV / dans le rapport).

## Pourquoi ce choix de clé (et pas le timestamp exact) ?

L'énoncé indique que l'heure peut varier de quelques secondes entre deux
enregistrements de la même partie. Inclure le timestamp exact dans la clé
empêcherait donc de détecter ces doublons. En ne gardant que
`(joueurs, jour, round)`, on capture l'identité "métier" de la partie —
telle que définie dans l'énoncé — indépendamment de petites variations de
timestamp. Le timestamp d'origine est conservé dans la valeur (pas dans la
clé) au cas où vous souhaitiez l'utiliser plus tard (ex: garder le plus
ancien des deux).

## Build & exécution

```bash
mvn clean package
hadoop fs -put dataset_brut.json /input/clash-royale/
hadoop jar target/clash-royale-cleaning-1.0.jar \
    ple.clashroyale.cleaning.DataCleaningDriver \
    /input/clash-royale/ /output/clash-royale-clean/ \
    -Dcleaning.num.reducers=16
```

Après exécution, consultez les compteurs Hadoop (`INVALID_JSON`,
`MISSING_FIELDS`, `INVALID_DECK_SIZE`, `DUPLICATES_REMOVED`, etc.) : ils
donnent directement la matière pour la partie qualité des données de votre
rapport, et peuvent être réutilisés tels quels dans le benchmark de la
Partie IV (en les comparant à la taille du dataset d'entrée).

## Limite connue / point à discuter dans le rapport

Si deux parties **distinctes** entre les deux mêmes joueurs ont, par
malchance, le même `round` le même jour (round réutilisé), elles seraient
fusionnées à tort. Si votre champ `round` est en réalité un identifiant
globalement unique par partie (et non un simple numéro de round dans un
combo/tournoi), ce risque est nul. À vérifier sur un échantillon réel des
données pour le justifier dans le rapport (point II.a).

---

# Partie II — Nœuds & Arêtes

Package : `ple.clashroyale.nodesedges` (même projet Maven, même jar final
`clash-royale-cleaning-1.0.jar`).

## Entrée

La sortie nettoyée de la Partie I (un match JSON normalisé par ligne).

## Notion d'"archetype"

Un archetype de taille `k` est une combinaison de `k` cartes extraite du
deck de 8 cartes d'un joueur. Son identifiant est **canonique** : les `k`
cartes sont triées puis jointes par `-` (ex: `0001-0004-0006-0007` pour
`k=4`), afin qu'un même sous-deck produise toujours le même identifiant
quel que soit l'ordre des cartes dans le deck d'origine.

- `k = 8` → un seul archetype par joueur : **le deck complet**. C'est le
  cas principal décrit dans l'énoncé ("L'analyse peut se faire en
  considérant les huit cartes du deck").
- `k = 1..7` → `C(8,k)` archetypes par joueur (les "sous-decks" évoqués en
  introduction).

La taille `k` est un paramètre du job (`archetypeSize`, argument 3 du
driver), ce qui permet de réutiliser exactement le même code pour l'étude
des decks complets et pour celle des archetypes de taille réduite.

## Nœuds

Pour chaque archetype rencontré (côté A ou côté B d'un match), on émet une
occurrence avec le résultat (victoire/défaite) **du joueur qui l'a joué**.
Le reducer agrège :

```
Archetype;Count;Win
```

`Count` = nombre de fois où cet archetype a été joué (par n'importe quel
joueur, dans n'importe quel match) ; `Win` = nombre de fois où le joueur
l'ayant joué a gagné.

## Arêtes

Pour chaque match, on prend le produit cartésien des archetypes de A et de
B : pour chaque paire `(archA, archB)`, on émet **deux** arêtes dirigées :
- `archA -> archB` avec le résultat de A (source = A) ;
- `archB -> archA` avec le résultat de B (source = B).

Le reducer agrège :

```
ArchetypeSource;ArchetypeTarget;Count;Win
```

- `Count(d1,d2)` = `Count(d2,d1)` par construction : c'est bien le nombre
  de fois où `d1` et `d2` se sont rencontrés (symétrique), comme requis
  pour comparer à la valeur théorique `N(d1)×N(d2)/Nall`.
- `Win(d1,d2)` = nombre de fois où **d1 a battu d2** (asymétrique) —
  c'est exactement "le nombre de victoires de di sur dj" demandé par
  l'énoncé. On a `Win(d1,d2) + Win(d2,d1) = Count(d1,d2)` (en supposant
  qu'il n'y a pas de match nul dans les données).
- Les auto-matchs (`d1 == d2`, ex: `0001;0001;44;19`) sont gérés
  naturellement : les deux émissions retombent sur la même clé.

Une classe utilitaire `SumCombiner` (utilisée aussi comme *combiner* côté
mapper) fait l'agrégation `count/win` ; c'est le même code qui sert de
combiner (réduction locale avant shuffle) et de brique du reducer final.

## ⚠️ Coût combinatoire — point clé pour le benchmark (Partie IV)

Le nombre d'émissions **par match** dépend fortement de `k` :

| k (taille archetype) | archetypes / joueur = C(8,k) | émissions "nœud" / match | émissions "arête" / match |
|---|---|---|---|
| 8 (deck complet) | 1 | 2 | 2 |
| 7 | 8 | 16 | 128 |
| 4 | 70 | 140 | 9 800 |
| 1 (carte seule) | 8 | 16 | 128 |

Pour `k=4`, chaque match génère **~9 800 émissions d'arêtes** (produit
cartésien 70×70×2). Sur 35 000 000 de parties, cela représente de l'ordre
de **3,4×10¹¹ émissions** avant combiner/shuffle — un volume à
mentionner explicitement dans l'analyse théorique des performances
(Partie IV.b) et à illustrer par un benchmark à `k` variable (Partie IV.c),
en plus du benchmark à taille de dataset variable déjà demandé. En
pratique, il est recommandé de :
- lancer d'abord `k=8` (cas principal, coût minimal) ;
- si le temps de cluster le permet, tester `k` décroissant (7, puis 6...)
  en observant la croissance du temps d'exécution et du volume
  intermédiaire (shuffle bytes) via les compteurs Hadoop, plutôt que de
  viser directement `k=1` sur les 35M de parties.

## Build & exécution

```bash
mvn clean package

# Deck complet (k=8, cas principal)
hadoop jar target/clash-royale-cleaning-1.0.jar \
    ple.clashroyale.nodesedges.NodesEdgesDriver \
    /output/clash-royale-clean/ /output/nodes-edges-k8/ 8 \
    -Dnodesedges.num.reducers=16

# Exemple avec des sous-decks de 4 cartes (coût combinatoire nettement plus élevé)
hadoop jar target/clash-royale-cleaning-1.0.jar \
    ple.clashroyale.nodesedges.NodesEdgesDriver \
    /output/clash-royale-clean/ /output/nodes-edges-k4/ 4 \
    -Dnodesedges.num.reducers=64
```

Résultat dans `/output/nodes-edges-k8/nodes-r-*` et `edges-r-*`.

---

# Partie III — Stats (jointure + prévision)

Package : `ple.clashroyale.stats`.

## Principe

Pour chaque arête `source;target;count;win` (Partie II), on calcule :

```
prevision = Count(source) x Count(target) / Nall
```

où `Nall` est le nombre total d'occurrences d'archetypes dans le jeu de
données (défini dans l'énoncé principal), calculé comme la **somme de la
colonne `Count` du fichier de nœuds**.

## Choix d'implémentation : jointure par diffusion, job map-only

Le fichier de **nœuds** est petit (une ligne par archetype distinct) par
rapport au fichier d'**arêtes** (une ligne par paire d'archetypes, bien
plus nombreuses). On utilise donc une **jointure par diffusion
("replicated join")** plutôt qu'une jointure côté reducer :

1. **`StatsDriver`** calcule `Nall` en lisant directement les fichiers de
   nœuds (petit volume, calcul fait une fois côté client/driver, hors
   MapReduce), puis diffuse ces mêmes fichiers de nœuds à toutes les
   tâches via le **Distributed Cache** (`job.addCacheFile`).
2. **`JoinStatsMapper`** charge cette table `archetype -> count` en
   mémoire dans `setup()`, puis traite le fichier d'arêtes **en flux** :
   pour chaque ligne, il retrouve `Count(source)` et `Count(target)` par
   simple lookup en mémoire (O(1)), calcule la prévision, et écrit le
   résultat.
3. **Aucun reducer** : c'est une jointure + un calcul arithmétique ligne à
   ligne, pas une agrégation. Le job tourne avec `job.setNumReduceTasks(0)`,
   ce qui évite un shuffle/sort inutile sur le plus gros des deux fichiers
   — un point à valoriser dans l'analyse théorique des performances
   (Partie IV.b) : jointure par diffusion en O(edges) sans shuffle, contre
   une jointure côté reducer qui aurait nécessité de trier/déplacer tout
   le fichier d'arêtes sur le réseau.

**Limite à mentionner dans le rapport** : cette approche suppose que le
fichier de nœuds tient en mémoire sur chaque tâche. C'est vrai pour le cas
principal (`k=8`, deck complet) et pour les petites valeurs de `k`, mais
pourrait ne plus l'être pour un `k` intermédiaire avec une très grande
diversité d'archetypes ; dans ce cas, il faudrait basculer vers une
jointure côté reducer (map-side tagging + secondary sort).

## Format de sortie

Le format suit exactement l'exemple de l'énoncé (y compris le fait que
`countSource` et `countTarget` sont regroupés dans un seul champ séparé
par une virgule, alors que les autres champs sont séparés par `;`) :

```
ArchetypeSource;ArchetypeTarget;count;win;countSource,countTarget;prevision
0001;0001;44;19;12051,12051;39.5
```

## Build & exécution

```bash
mvn clean package

hadoop jar target/clash-royale-cleaning-1.0.jar \
    ple.clashroyale.stats.StatsDriver \
    /output/nodes-edges-k8/ /output/stats-k8/
```

(`/output/nodes-edges-k8/` doit contenir à la fois les fichiers
`nodes-r-*` et `edges-r-*` produits par la Partie II.)

---

# Partie IV — Analyse statistique locale (Python)

Comme précisé dans l'énoncé, cette partie ne nécessite pas Hadoop : le
fichier de stats de la Partie III est rapatrié en local (`hadoop fs -get`)
et analysé avec Python.

Script : `python/analyze_stats.py`. Il calcule une régression linéaire
entre valeur mesurée (`count`) et valeur prévue (`prevision`), affiche la
pente (1.0 attendu si le système est parfaitement aléatoire) et le R²,
et produit un nuage de points en échelle log-log (les counts s'étalent sur
plusieurs ordres de grandeur entre decks très joués et decks rares).

```bash
hadoop fs -getmerge /output/stats-k8/part-m-* stats-k8.txt
cd python
pip install -r requirements.txt
python analyze_stats.py ../stats-k8.txt -o matchups_scatter.png
```

Pistes d'analyse complémentaires à mentionner dans le rapport (non
codées ici, à ajouter selon vos observations) :
- comparer la pente obtenue pour différentes tailles d'archetype `k`
  (deck complet vs. sous-decks) ;
- regarder si l'écart à la droite `y=x` est corrélé au niveau moyen des
  cartes du deck ou au clan (biais potentiel du matchmaking au-delà du
  simple aléatoire) ;
- analyser la distribution des taux de victoire (`win/count`) par
  archetype pour repérer des decks structurellement avantagés.
