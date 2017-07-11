from flask import Flask, render_template, request
from bokeh.embed import components
import pyspark 
from pyspark import SparkContext
from pyspark.sql import SQLContext
from bokeh.models import HoverTool, ColumnDataSource, Legend, LegendItem
from bokeh.plotting import figure, show
from bokeh.models.glyphs import Rect
from bokeh.core.properties import field

app = Flask(__name__)
sc = SparkContext()
sql = SQLContext(sc)

def create_team(season1,season2):
    teamDF = (sql.read.format("com.databricks.spark.csv").\
            option("header","true").load(data_opath + "dreamTeam" + season1+"_"+season2+".csv"))
    yaxis = [str(x) for x in range(1, 7)]
    xaxis = ["a","b"]
    player=[str(i.name) for i in teamDF.collect()]
    batsman_overall=[int(i.batsmanrating) for i in teamDF.collect()]
    bowler_overall=[int(i.bowlerrating) for i in teamDF.collect()]
    fielder_overall=[int(i.fielderrating) for i in teamDF.collect()]
    strikerate=[float(i.strikerate) for i in teamDF.collect()]
    economyrate=[float(i.economyrate) for i in teamDF.collect()]
    ycoordinate =["Batsman","Batsman","Batsman","Batsman","Batsman",\
        "Wicket-Keeper","Bowler","Bowler","Bowler","Bowler","Bowler"]

    colormap = {
        
        "Batsman"      : "#0092CD",
        "Wicket-Keeper": "#DB0033",
        "Bowler"       : "#4FC730",
        
    }

    source = ColumnDataSource(
        data=dict(
            role = [i for i in ycoordinate],
            xpoints = ["a","a","a","a","a","a","b","b","b","b","b"],
            ypoints = [2,3,4,5,6,1,6,5,4,3,2],
            player = [i for i in player],
            batscore = [i for i in batsman_overall],
            bowlscore = [i if i != 0 else 2 for i in bowler_overall],
            fieldscore = [i for i in fielder_overall],
            srate = [i if i!= 100.0 else "NA" for i in strikerate],
            erate = [i if i!= 100.0 else "NA" for i in economyrate],
            type_color = [colormap[x] for x in ycoordinate],
        )
    )

    p = figure(title = "Dream Team", x_range = xaxis, y_range = list(reversed(yaxis)), tools ="hover")
    p.plot_width = 800
    p.plot_height = 500
    p.toolbar_location = None
    p.outline_line_color = None
    p.axis.visible = False

    rect = Rect(x="xpoints", y="ypoints", width=0.9, height=0.9, fill_color="type_color")
    rect_render = p.add_glyph(source, rect)

    legend = Legend(items=[LegendItem(label=field("role"), renderers=[rect_render])])
    p.add_layout(legend, 'right')

    text_props = {
        "source": source,
        "angle": 0,
        "color": "black",
        "text_align": "center",
        "text_baseline": "middle",
    }

    p.text(x="xpoints", y="ypoints", text="player",
           text_font_style="bold", text_font_size="12pt", **text_props)

    p.grid.grid_line_color = None

    p.select_one(HoverTool).tooltips = [
        ("player","@player"),
        ("role","@role"),
        ("batting","@batscore"),
        ("bowling","@bowlscore"),
        ("fielding","@fieldscore"),
        ("strikerate","@srate"),
        ("economyrate","@erate")
    ]
    return p

year_list = ["2008","2009","2010","2011","2012","2013","2014","2015","2016"]

@app.route('/DreamTeam/webapp')
def returnDreamTeamWebApp():

    lbound= request.args.get("lbound")
    ubound= request.args.get("ubound")
    if lbound == None:
       lbound = "2008"
    
    if ubound == None:
        ubound = "2016"

    lseason = int(lbound)
    useason = int(ubound)
    if(lseason > useason):
        lseason^=useason
        useason^=lseason
        lseason^=useason
    lbound = str(lseason)
    ubound = str(useason)
    
    plot = create_team(lbound,ubound)
        

    script, div = components(plot)
    return render_template("dreamteam.html", script=script, div=div, year_list=year_list,lbound=lbound,ubound=ubound)

if __name__ == '__main__':
    app.run(port=5000, debug=True)
