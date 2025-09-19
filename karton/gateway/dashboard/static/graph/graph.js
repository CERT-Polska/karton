var dom = document.getElementById("graph-container");
var myChart = echarts.init(dom);
var app = {};
var allNodes;
var nodeData = {};
var isLabelsHidden = false;

const attr = {
  version: "0",
  info: "1",
};

const loadingOpts = {
  text: 'loading graph, please wait'
};

const arrayToObject = (array) =>
  array.reduce((obj, item) => {
    obj[item.id] = item;
    return obj;
  }, {});

window.onresize = function() {
  myChart.resize();
};

option = null;
myChart.showLoading('default', loadingOpts);
$.get(dom.getAttribute('data-generate-url'), function(raw_graph) {
  myChart.hideLoading();
  myChart.resize();

  var graph = echarts.dataTool.gexf.parse(raw_graph);
  allNodes = arrayToObject(graph.nodes);

  option = {
    title: {
      top: "bottom",
      left: "right",
    },
    feature: {
      magicType: {
        type: ["line", "bar", "stack", "tiled"],
      },
    },
    tooltip: {
      formatter: function (params) {
        if (params.dataType == "node") {
          var nodeDescColor = "#3399F3";
          var colorSpan =
            '<span style="display:inline-block;margin-left:5px;border-radius:10px;width:9px;height:9px;background-color:' +
            nodeDescColor +
            '"></span>';
          // is node
          res = params.data.id + colorSpan;
        } else if (params.dataType == "edge") {
          // is edge
          res =
            allNodes[params.data.source].name +
            " → " +
            allNodes[params.data.target].name;
        }
        return res;
      },
    },
    legend: [],
    animation: true,
    animationDuration: 1500,
    scaleLimit: {},
    animationEasingUpdate: "quinticInOut",
    dataZoom: [
      {
        type: "inside",
      },
      {
        type: "inside",
      },
    ],
    xAxis: {
      show: false,
      scale: true,
      silent: true,
      type: "value",
    },
    yAxis: {
      show: false,
      scale: true,
      silent: true,
      type: "value",
    },

    series: [
      {
        name: "Karton map",
        type: "graph",
        layout: "force",
        force: {
          initLayout: "circular",
          edgeLength: 1700,
          repulsion: 150000,
          gravity: 0.2,
        },
        zoom: 0.1,
        edgeSymbol: ["circle", "arrow"],
        edgeSymbolSize: [4, 9],
        data: graph.nodes,
        links: graph.links,
        roam: true,
        draggable: true,
        itemStyle: {
          normal: {
            borderColor: "#fff",
            borderWidth: 1,
            shadowBlur: 10,
            shadowColor: "rgba(0, 0, 0, 0.3)",
          },
        },
        labelLayout: {
          hideOverlap: true
        },
        label: {
          position: "outside",
          show: true,
          formatter: "{b}",
        },
        lineStyle: {
          color: "source",
          curveness: 0.05,
          width: 2,
        },
        emphasis: {
          lineStyle: {
            width: 4,
          },
          focus: 'adjacency',
        },
      },
    ],
  };

  myChart.setOption(option);
});

if (option && typeof option === "object") {
  myChart.setOption(option, true);
}

myChart.on("dataZoom", function (params) {
  var start = params.batch[0].start;
  var end = params.batch[0].end;

  if (
    myChart.getOption().series[0].zoom <= 0.1 &&
    myChart.getOption().series[0].zoom != 1 &&
    !isLabelsHidden
  ) {
    myChart.setOption({
      series: [
        {
          label: {
            show: false,
          },
          force: {
            friction: 0.1,
          },
        },
      ],
    });
    isLabelsHidden = true;
  } else if (
    myChart.getOption().series[0].zoom > 0.1 &&
    myChart.getOption().series[0].zoom != 1 &&
    isLabelsHidden
  ) {
    myChart.setOption({
      series: [
        {
          label: {
            show: true,
          },
          force: {
            friction: 0.1,
          },
        },
      ],
    });
    isLabelsHidden = false;
  }
});

myChart.on(
  "click",
  {
    dataType: "node",
  },
  (params) => {
    if (params.dataType === "node") {
      showInformation(params.data, params.color);
    }
    params.event.stop();
  }
);

myChart.on("mousemove", (params) => {
  if (params.dataType === "node") {
    myChart.getZr().setCursorStyle("pointer");
  } else {
    myChart.getZr().setCursorStyle("default");
  }
});

$("#graph-container").click(function () {
  var $lefty = $(".side-menu");
  $lefty.animate({
    left: -300,
  });

  clearWindow();
});

function showInformation(node, color) {
  clearWindow();

  var $lefty = $(".side-menu");
  $lefty.name = $lefty.find(".name");
  $lefty.version = $lefty.find(".version");
  $lefty.about = $lefty.find(".about");

  $lefty.name.html("<b>Name: </b>" + node.name);

  if (node.hasOwnProperty("attributes") && node.attributes != null) {
    if (
      attr.version in node.attributes &&
      node.attributes[attr.version] != "none"
    ) {
      $lefty.version.html(
        "<b>Version: </b>" + node.attributes[attr.version] + "<br/>"
      );
    }

    if (attr.info in node.attributes && node.attributes[attr.info] != "none") {
      spacesRemoved = node.attributes[attr.info].replace(/  +/g, "");
      sanitized = DOMPurify.sanitize(spacesRemoved);
      $lefty.about.html(marked.parse(sanitized));
    }
  }

  $lefty.animate({
    left: 0,
  });
}

function clearWindow() {
  var $lefty = $(".side-menu");

  $lefty.name = $lefty.find(".name");
  $lefty.version = $lefty.find(".version");
  $lefty.about = $lefty.find(".about");

  $lefty.name.html("");
  $lefty.version.html("");
  $lefty.about.html("");
}
