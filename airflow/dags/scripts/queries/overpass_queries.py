# National borders
ph_query = 'relation["boundary"="administrative"]\
["admin_level"="2"]["name:en"="Philippines"]; out geom;'

# Regional borders
r_query = 'area(3600443174)->.searchArea;\
(relation["admin_level"="3"]["boundary"="administrative"]\
(area.searchArea);); out body geom;'

# Provincial borders
p_query = 'area(3600443174)->.searchArea;\
(relation["admin_level"="4"]["boundary"="administrative"]\
(area.searchArea);); out body geom;'

# City and municipality borders
cm_query = 'area(3600443174)->.searchArea;\
(relation["admin_level"="6"]["boundary"="administrative"]\
(area.searchArea);); out body geom;'
