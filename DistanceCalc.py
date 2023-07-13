from geopy import distance
import folium
import webbrowser


#KoordinatenvonSanFrancisco
def distanceCalc(coords_1,coords_2,district):
    point1=coords_1
    point2=coords_2
    latitude1=point1[0]
    longitude1=point1[1]
    latitude2=point2[0]
    longitude2=point2[1]

    #ErstelleeineKartevonSanFrancisco
    sf_map=folium.Map(location=point1,zoom_start=13,tiles='CartoDBpositron')

    #FügeMarkerfürdiePunktehinzu
    folium.Marker(location=[latitude1,longitude1],popup=district).add_to(sf_map)
    folium.Marker(location=[latitude2,longitude2],popup=district).add_to(sf_map)

    #VerbindediePunktemiteinerrotenLinie
    folium.PolyLine(locations=[(latitude1,longitude1),(latitude2,longitude2)],color='red').add_to(sf_map)

    #BerechnedieDistanzzwischendenPunkten
    distances = distance.distance(point1, point2).km

    #FügedieDistanzalsTextaufderKartehinzu
    folium.map.Marker(
    [(latitude1+latitude2)/2,(longitude1+longitude2)/2],
    icon=folium.DivIcon(html=f"<divstyle='font-size:25pt;margin-left:150px;font-weight:bold;'>distance:{distances:.2f}kilometer</div>"),

    ).add_to(sf_map)

    #ZeigedieKartean
    sf_map.save('san_francisco_map.html')

    #ÖffnedieHTML-DateiimWebbrowser
    webbrowser.open('san_francisco_map.html')

distanceCalc((37.7564864109309,-122.406539115148), (37.775420706711,-122.403404791479), "Mission")