<< PlotLegends`
MyMakePlot[xpoints_, ypoints_, xaxis_, yaxis_, title_] := 
   ListPlot[ypoints, Joined -> True, PlotMarkers -> Automatic, 
    PlotLegend -> {"libcuckoo", "Intel TBB", "gcc 4.8 STL"}, 
    LegendShadow -> None, LegendPosition -> {1, 0}, 
    FrameTicks -> {{Automatic, None}, {xpoints, None}}, 
    DataRange -> {First[xpoints], Last[xpoints]}, Frame -> True, 
    FrameLabel -> {{yaxis, ""}, {xaxis, title}}]
MyMakePlotNoLegend[xpoints_, ypoints_, xaxis_, yaxis_, title_] := 
   ListPlot[ypoints, Joined -> True, PlotMarkers -> Automatic, 
    FrameTicks -> {{Automatic, None}, {xpoints, None}}, 
    DataRange -> {First[xpoints], Last[xpoints]}, Frame -> True, 
    FrameLabel -> {{yaxis, ""}, {xaxis, title}}]
MakeGraph[y_, title_] := MyMakePlotNoLegend[x, y, xlabel, ylabel, title]
MakeGraphLegend[y_, title_] := MyMakePlot[x, y, xlabel, ylabel, title]
