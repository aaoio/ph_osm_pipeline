# Regions
r_query = """
SELECT DISTINCT ?item ?itemLabel ?population ?within ?withinLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  {
    SELECT DISTINCT ?item ?population ?within WHERE {
      ?item p:P31 ?statement0.
      ?statement0 (ps:P31) wd:Q24698.
      ?item wdt:P1082 ?population.
      ?item wdt:P131 ?within.
    }
  }
}
"""

# Provinces
p_query = """
SELECT DISTINCT ?item ?itemLabel ?population ?within ?withinLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  {
    SELECT DISTINCT ?item ?population ?within WHERE {
      ?item p:P31 ?statement0.
      ?statement0 (ps:P31) wd:Q24746.
      ?item wdt:P1082 ?population.
      ?item wdt:P131 ?within.
    }
  }
}
"""

# Cities
cm_query = """
SELECT DISTINCT ?item ?itemLabel ?within ?withinLabel ?population ?area ?instanceofLabel ?incomeclassLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  {
    SELECT DISTINCT ?item ?within ?population ?area ?instanceof ?incomeclass WHERE {
      {
        ?item p:P31 ?statement0.
        ?statement0 (ps:P31/(wdt:P279*)) wd:Q24764.
      }
      UNION
      {
        ?item p:P31 ?statement1.
        ?statement1 (ps:P31/(wdt:P279*)) wd:Q106078286.
      }
      UNION
      {
        ?item p:P31 ?statement2.
        ?statement2 (ps:P31/(wdt:P279*)) wd:Q29946056.
      }
      UNION
      {
        ?item p:P31 ?statement3.
        ?statement3 (ps:P31/(wdt:P279*)) wd:Q106079704.
      }
      ?item wdt:P131 ?within.
      ?item wdt:P1082 ?population.
      ?item wdt:P2046 ?area.
      ?item wdt:P31 ?instanceof.
      ?item wdt:P1879 ?incomeclass.
      ?item p:P17 ?statement4.
      ?statement4 (ps:P17/(wdt:P279*)) wd:Q928.
    }
  }
}
"""
