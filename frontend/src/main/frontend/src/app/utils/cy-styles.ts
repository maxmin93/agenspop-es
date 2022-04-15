import * as _ from 'lodash';

export const CY_STYLES = [
  {
  "selector": "core",
  "css": {
    "active-bg-opacity": 0,

    "selection-box-color": "#11bf1c",
    "selection-box-opacity": 0.25,
    "selection-box-border-color": "#aaa",
    "selection-box-border-width": 1
  }}, {
  "selector": ":parent",
  "css":{
    "background-opacity": 0.333,
    "z-compound-depth": "bottom",
    "border-width":"1",
    "border-color":"#888",
    "border-style":"dotted",
    "padding-top": "10px",
    "padding-left": "10px",
    "padding-bottom": "10px",
    "padding-right": "10px",
    "text-valign": "top",
    "text-halign": "center",
    "background-color": "#B8BdB1"
  }},

  {
  "selector": "node",
  "css": {
    "width": 16,    //function(e){ return 20 / e._private.cy._private.zoom; },
    "height": 16,   //function(e){ return 20 / e._private.cy._private.zoom; },
    "border-width": 1,
    "border-color": "#FFFFFF",
    // "background-color": function(e){ return e.scratch('_color'); }
  }},{
  "selector": "node:selected",
  "css": {
    "border-width": 4,
    "background-color": "#FFFFFF",   // not working, why??
    "z-index": 9,
    "width": 24,
    "height": 24,
    // "border-color": function(e){ return e.scratch('_color'); },
  }},{
  "selector": "node.highlighted",
  "css": {
    "border-width": 4,
    "background-color": "#FFFFFF",
    "z-index": 9,
    // "border-color": function(e){ return e.scratch('_color'); },
  }},{
  "selector": "node:locked",
  "css": {
    "border-color": "#d64937",
    "border-width": 3,
    "background-color": "#d64937"
  }}, {
  "selector": "node.expanded",
  "css": {
    "opacity": 0.6,
    "border-color": "black",
    "border-width": 1,
  }}, {
  "selector": "node.captionId",
  "css": {
    'content': 'data(id)',
    'text-wrap': 'wrap',
    'text-max-width': '80px',
    'text-opacity': 0.6,
    'font-weight': 400,
    'font-size': 10
  }}, {
  "selector": "node.captionLabel",
  "css": {
    'content': 'data(label)',
    'text-wrap': 'wrap',
    'text-max-width': '80px',
    'text-opacity': 0.6,
    'font-weight': 400,
    'font-size': 10
  }}, {
    "selector": "node.captionName",
    "css": {
      'content': function(e){ return e.data('properties').hasOwnProperty('name') ? e.data('properties')['name'] : ''; },
      'text-wrap': 'wrap',
      'text-max-width': '80px',
      'text-opacity': 0.6,
      'font-weight': 400,
      'font-size': 10
  }}, {
  "selector": "node.icon",
  "css": {
    // "border-width": 0,
    "background-fit": "contain",  // none, contain, cover
    "background-clip": "none",    // none, node
    "background-width": 16,
    "background-height": 16,
    "background-opacity": 0,
    // "background-image": function(e){ return 'assets/icons/'+e.scratch('_icon').path; }
  }}, {
  "selector": "node.seed-semi",
  "css": {
    "border-width": 6,
    "width": 32,
    "height": 32,
  }}, {
  "selector": "node.seed",
  "css": {
    "z-index": 9999,
    "border-color": "#e600e6",
    "border-width": 6,
    "width": 32,
    "height": 32,
  }}, {
  "selector": "node.gcunit",
  "css": {
    "border-style": "double",
    "border-color": "#a9a9a9",
  }},

  {
  "selector": "edge",
  "css": {
    "width": 1,
    "opacity": 0.5,
    "arrow-scale": 0.7,
    "curve-style": "bezier",
    "target-arrow-shape": "vee",
    "source-arrow-shape": "none",
    "line-fill": "linear-gradient",
    "line-gradient-stop-positions": "50%",
    "line-style": "solid",
    // "target-arrow-color": function(e){ return e.scratch('_color')[1]; },
    // "line-gradient-stop-colors": function(e){ return e.scratch('_color'); }
  }}, {
  "selector": "edge:selected",
  "css": {
    "width": 3,
    "target-arrow-shape": "triangle",
    "opacity": 0.8,
    "z-index": 9
  }}, {
  "selector": "edge.highlighted",
  "css": {
    "width": 3,
    "target-arrow-shape": "triangle",
    "opacity": 0.8,
    "z-index": 9
  }}, {
  "selector": "edge.gclink",
  "css": {
    "line-style": "dashed",
    "opacity": 1
  }},

  {
  "selector": "node.faded",
  "css": {
    "opacity": 0.08
  }},{
  "selector": "edge.faded",
  "css": {
    "opacity": 0.05
  }},{
    "selector": ".hidden",
  "css": {
    "display": "none"
  }},{
  "selector": ".traveled",
  "css": {
    "background-color": "#11bf1c",
    "line-color": "#11bf1c",
    "target-arrow-color": "black",
    "transition-property": "background-color, line-color, target-arrow-color",
    "transition-duration": "0.2s"
  }},{
  "selector": ".eh-handle",
  "css": {
    "background-color": "red",
    "width": 12,
    "height": 12,
    "shape": "ellipse",
    "overlay-opacity": 0,
    "border-width": 12,
    "border-opacity": 0
  }},{
  "selector": ".eh-hover",
  "css": {
    "background-color": "red"
  }},{
  "selector": ".eh-source",
  "css": {
    "border-width": 2,
    "border-color": "red"
  }},{
  "selector": ".eh-target",
  "css": {
    "border-width": 2,
    "border-color": "red"
  }},{
  "selector": ".eh-preview, .eh-ghost-edge",
  "css": {
    "background-color": "red",
    "line-color": "red",
    "target-arrow-color": "red",
    "source-arrow-color": "red"
  }},{
  "selector": ".eh-ghost-edge.eh-preview-active",
  "css": {
    "opacity": 0
  }},

  {"selector": ".invisible",  "style":{ "display": "none" }},   // none or element

  //////////////////////////////////////////////////////////////////////

  {"selector": "node.label0",  "style":{ "background-color": "#0e2134" }},
  {"selector": "node.label1",  "style":{ "background-color": "#1898d7" }},
  {"selector": "node.label2",  "style":{ "background-color": "#1d5b6d" }},
  {"selector": "node.label3",  "style":{ "background-color": "#479325" }},
  {"selector": "node.label4",  "style":{ "background-color": "#8dc99d" }},
  {"selector": "node.label5",  "style":{ "background-color": "#505142" }},
  {"selector": "node.label6",  "style":{ "background-color": "#d4c585" }},
  {"selector": "node.label7",  "style":{ "background-color": "#ffbd5d" }},
  {"selector": "node.label8",  "style":{ "background-color": "#cb234b" }},
  {"selector": "node.label9",  "style":{ "background-color": "#b95f3b" }},

  {"selector": "node.label10", "style":{ "background-color": "#1b3f84" }},
  {"selector": "node.label11", "style":{ "background-color": "#3d9ad6" }},
  {"selector": "node.label12", "style":{ "background-color": "#224a50" }},
  {"selector": "node.label13", "style":{ "background-color": "#80ab21" }},
  {"selector": "node.label14", "style":{ "background-color": "#008c6d" }},
  {"selector": "node.label15", "style":{ "background-color": "#726e4b" }},
  {"selector": "node.label16", "style":{ "background-color": "#eeca58" }},
  {"selector": "node.label17", "style":{ "background-color": "#f4b034" }},
  {"selector": "node.label18", "style":{ "background-color": "#7a2f4c" }},
  {"selector": "node.label19", "style":{ "background-color": "#f1632c" }},

  {"selector": "node.label20", "style":{ "background-color": "#005aa8" }},
  {"selector": "node.label21", "style":{ "background-color": "#66b7e6" }},
  {"selector": "node.label22", "style":{ "background-color": "#3d4c36" }},
  {"selector": "node.label23", "style":{ "background-color": "#b5d56e" }},
  {"selector": "node.label24", "style":{ "background-color": "#005a3f" }},
  {"selector": "node.label25", "style":{ "background-color": "#a09857" }},
  {"selector": "node.label26", "style":{ "background-color": "#fdd656" }},
  {"selector": "node.label27", "style":{ "background-color": "#e34a69" }},
  {"selector": "node.label28", "style":{ "background-color": "#9e423d" }},
  {"selector": "node.label29", "style":{ "background-color": "#f27148" }},

  {"selector": "node.label30", "style":{ "background-color": "#5c7bbb" }},
  {"selector": "node.label31", "style":{ "background-color": "#0085ae" }},
  {"selector": "node.label32", "style":{ "background-color": "#1d4c14" }},
  {"selector": "node.label33", "style":{ "background-color": "#a1d09e" }},
  {"selector": "node.label34", "style":{ "background-color": "#426b61" }},
  {"selector": "node.label35", "style":{ "background-color": "#a39778" }},
  {"selector": "node.label36", "style":{ "background-color": "#ffe55b" }},
  {"selector": "node.label37", "style":{ "background-color": "#d2103d" }},
  {"selector": "node.label38", "style":{ "background-color": "#744f43" }},
  {"selector": "node.label39", "style":{ "background-color": "#f7883e" }},

  //////////////////////////////////////////////////////////////////////

];

export const CY_PANZOOM = {
  zoomFactor: 0.05, // zoom factor per zoom tick
  zoomDelay: 45, // how many ms between zoom ticks
  minZoom: 0.01, // min zoom level
  maxZoom: 10, // max zoom level
  fitPadding: 50, // padding when fitting
  panSpeed: 10, // how many ms in between pan ticks
  panDistance: 10, // max pan distance per tick
  panDragAreaSize: 75, // the length of the pan drag box in which the vector for panning is calculated (bigger = finer control of pan speed and direction)
  panMinPercentSpeed: 0.25, // the slowest speed we can pan by (as a percent of panSpeed)
  panInactiveArea: 3, // radius of inactive area in pan drag box
  panIndicatorMinOpacity: 0.5, // min opacity of pan indicator (the draggable nib); scales from this to 1.0
  autodisableForMobile: true, // disable the panzoom completely for mobile (since we don't really need it with gestures like pinch to zoom)
  // additional
  zoomOnly: false, // a minimal version of the ui only with zooming (useful on systems with bad mousewheel resolution)
  fitSelector: undefined, // selector of elements to fit
  animateOnFit: function(){ // whether to animate on fit
    return false;
  },
  // icon class names
  sliderHandleIcon: 'fa fa-minus',
  zoomInIcon: 'fa fa-plus',
  zoomOutIcon: 'fa fa-minus',
  resetIcon: 'fa fa-expand'
};

export const CY_EVT_INIT:Function = function(cy:any){
  cy.$api = {};

  // cy.$api.panzoom = cy.panzoom(CY_PANZOOM);

  // ==========================================
  // ==  cy events 등록
  // ==========================================

  // 마우스가 찍힌 위치를 저장 (해당 위치에 노드 등을 생성할 때 사용)
  cy.on('cxttapstart', function(e){
    cy.scratch('_position', e.position);
  });

  // ** 여기서는 공통의 탭이벤트만 처리
  cy.on('tap', function(e){
    // 바탕화면 탭 이벤트
    if( e.target === cy ){
      cy.elements(':selected').unselect();
      cy.scratch('_selected', null);
    }
  });

  // ** node 선택을 위한 편의 기능 (뭉쳤을때)
  cy.on('mouseover', 'node', function(e){
    if( e.target && !e.target.selected() ) e.target.style('z-index', 1);
  });
  cy.on('mouseout', 'node', function(e){
    if( e.target && !e.target.selected() ) e.target.style('z-index', 0);
  });

  // ==========================================
  // ==  cy utilities 등록
  // ==========================================

  cy.$api.findById = function(id){
    let ele = cy.elements().getElementById(id);
    return ele.nonempty() ? ele : undefined;
  };

  // layouts = { bread-first, circle, cose, cola, 'klay', 'dagre', 'cose-bilkent', 'concentric" }
  // **NOTE: euler 는 속도가 빠르지만 간혹 stack overflow 문제를 일으킨다. 사용 주의!!
  cy.$api.changeLayout = function(layout='cose', options=undefined){
    let elements = cy.elements(':visible');
    let boundingBox = undefined;
    let partial_layout = false;
    let animation_enabled = 'false';
    let padding = 50;
    if( options ){
      if( options.hasOwnProperty('elements') && options['elements'] ){ 
        elements = options['elements'];
        partial_layout = true;                  // 부분 레이아웃 적용
      }
      if( options.hasOwnProperty('boundingBox') && options['boundingBox'] ) 
        boundingBox = options['boundingBox'];
      if( options.hasOwnProperty('animate') && options['animate'] ) 
        animation_enabled = options['animate'];
      if( options.hasOwnProperty('padding') && options['padding'] ) 
        padding = options['padding'];
    }

    let layoutOption = {
      "name": layout,
      "fit": (partial_layout) ? false : true, 
      "padding": padding, 
      "boundingBox": (partial_layout) ? boundingBox : undefined, 
      "nodeDimensionsIncludeLabels": true, randomize: false,
      "animate": animation_enabled == 'true' ? 'end' : false,
      "refresh": 30, "animationDuration": 800, "maxSimulationTime": 2800,
      "ready": function(){
        if( options && options.hasOwnProperty('ready') ) (options.ready)();
      }, 
      "stop": function(){ 
        if( options && options.hasOwnProperty('stop') ) (options.stop)();
        Promise.resolve(null).then(()=>{
          if( partial_layout ) cy.fit( cy.elements(':visible'), 50 );
        });
      },
      // for euler
      "springLength": edge => 120, springCoeff: edge => 0.0008,
    };

    // adjust layout
    let layoutHandler = elements.layout(layoutOption);
    layoutHandler.run();
  }

/*
  // on&off control: cy.edgehandles('enable') or cy.edgehandles('disable')
  cy.$api.edge = cy.edgehandles({
      preview: true,
      hoverDelay: 150,
      handleNodes: 'node',
      handlePosition: function( node ){ return 'middle top'; },
      handleInDrawMode: false,
      edgeType: function( sourceNode, targetNode ){ return 'flat'; },
      loopAllowed: function( node ){ return false; },
      nodeLoopOffset: -50,
      edgeParams: function( sourceNode, targetNode, i ){
        return { classes: 'new'};
      },
    });
  cy.$api.edge.disable();
  // **참고 https://github.com/cytoscape/cytoscape.js-edgehandles
  // cy.on('ehcomplete', (event, sourceNode, targetNode, addedEles) => {
  //   let { position } = event;
  //   // ...
  // });
*/

  cy.$api.unre = cy.undoRedo({
      isDebug: false, // Debug mode for console messages
      undoableDrag: true, // Whether dragging nodes are undoable can be a function as well
    });

  // Public Property : APIs about view and undoredo
  cy.$api.view = cy.viewUtilities({
    neighbor: function(node){
        return node.openNeighborhood();
    },
    neighborSelectTime: 600
  });

  // 이웃노드 찾기 : labels에 포함된 label을 갖는 node는 제외
  cy.$api.findNeighbors = function( node, uniqLabels, maxHops, callback=undefined ){
    // empty collection
    let connectedNodes = cy.collection();
    // if limit recursive, stop searching
    if( maxHops <= 0 ) return connectedNodes;

    // 새로운 label타입의 edge에 대한 connectedNodes 찾기
    // 1) 새로운 label 타입의 edges (uniqLabels에 없는)
    let connectedEdges = node.connectedEdges().filter(function(ele, i){
      return ele.visible() && uniqLabels.indexOf(ele.data('label')) < 0;
    });
    // 2) edge에 연결된 node collection을 merge (중복제거)
    for( let i=0; i<connectedEdges.size(); i+=1 ){
      connectedNodes = connectedNodes.merge( connectedEdges[i].connectedNodes() );
    }
    // connectedNodes = connectedNodes.difference(node);                           // 자기 자신은 빼고
    // 3) uniqLabels 갱신
    connectedEdges.forEach(elem => {
      if( uniqLabels.indexOf(elem.data('label')) < 0 ){
        uniqLabels.push(elem.data('label'));
      } 
    });

    // 4) append recursive results
    maxHops -= 1;
    connectedNodes.difference(node).forEach(elem => {
      let collection = cy.$api.findNeighbors(elem, uniqLabels.slice(0), maxHops);
      connectedNodes = connectedNodes.merge( collection );
    });

    // 5) callback run
    if( callback !== undefined ) callback();

    // 6) return connectedNodes
    return connectedNodes;
  };

  cy.$api.randomId = function(prefix=undefined){
    let id = !prefix ? "_id_" : prefix+'_';
    let possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for( let i=0; i < 8; i++ )
      id += possible.charAt(Math.floor(Math.random() * possible.length));
    return id;
  };

  cy.$api.grouping = function(members=undefined, target=undefined, title=undefined){
    let nodes = cy.nodes(':selected');
    if( members && !members.empty() ) nodes = members;
    if( nodes.empty() ) return;

    let parentPos = nodes.boundingBox();
    let edges = nodes.connectedEdges();

    cy.elements(':selected').unselect();
    nodes.remove();   // 우선순위 문제 때문에 삭제했다가 맨 나중에 다시 추가

    if( !target ){
      let parentId = cy.$api.randomId(cy.scratch('_datasource'));
      let parent = { "group": "nodes"
                  , "data": { 
                    "id": parentId, "name": (title)?title:'group', "parent": undefined,
                    "props": { "$$size": nodes.size(), "$$members": nodes.map(x=>x.id()) }
                  }
                  , "position": { "x": (parentPos.x1+parentPos.x2)/2, "y": (parentPos.y1+parentPos.y2)/2 } 
                  , "selectable": true       // 선택 대상에 포함 (2018-12-03)
                }
      target = cy.add(parent);
    }

    cy.batch(() => { 
      target.style('width', parentPos.x2-parentPos.x1 );
      target.style('height', parentPos.y2-parentPos.y1 );
      target.scratch('_members', nodes);    // save memebers

      nodes.forEach(v => {
        v._private.data.parent = target.id();
      });
      cy.add(nodes); 
      cy.add(edges); 
    });

    return target;
  }

  cy.$api.degrouping = function(target=undefined){
    if( !target || !target.isNode() ) {
      let nodes = cy.nodes(':selected');
      if( nodes.empty() || !nodes[0].isParent() ) return;
      target = nodes[0];
    }

    let children = target.children().nodes();
    let edges = children.connectedEdges();
    children.forEach(e => {
      e._private.data.parent = undefined;
    });
    target.remove();
    cy.add(children);
    cy.add(edges);

    return children;    // nodes
  }

};