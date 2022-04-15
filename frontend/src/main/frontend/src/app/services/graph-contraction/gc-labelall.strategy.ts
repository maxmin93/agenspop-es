import { GcStrategy } from './gc-strategy';

import * as _ from 'lodash';
import * as d3 from 'd3-scale';

export class GcLabelAllStrategy implements GcStrategy {

  private cy: any;                      // cytoscape object
  private label: string;

  private singles: any;                 // initial singles
  private elements: any;                // initial all elements
  private components:any[];             // array of component(= nodes & edges)
  private bins:any[][];                 // array of component(= array of nodes with same rank)
  private topRank:number = 5;           // size of rank => quanitle.range([0,1,2,3,4])

  private seeds:any[];                  // array of created seeds with each components
  // private seed_links:any[];             // array of links connected label nodes and seed with each components

  constructor(cy:any, label:string){
    this.cy = cy;
    this.label = label;

    // 0) define elements for graph-contraction
    //    - exlucde components of single node
    this.singles = cy.nodes(':visible').filter(e=>e.connectedEdges(':visible').size()==0);
    this.elements = cy.elements(':visible').difference( this.singles );
    // console.log('0) visible elements', singles, elements);

    // ** prepare binning
    // ---------------------------
    // 1) get components => array of nodes and edges
    //    - set: scratch('_component', index)
    //    - label nodes 가 없는 component 제거 => singles 에 추가
    //    - seeds 에 label nodes 을 저장 => 나중에 topRank 부여
    this.components = this.split_components(this.elements, label);
    // label nodes 가 많은 component 에는 temp seed 생성 => 보기 좋으라고 (데코)
    // ..
    // console.log('1) split_components='+this.components.length, this.components);

    // singles 화면에서 숨기기
    this.singles.style('display','none');

    // 2) calc centralities with user option
    //    - set: scratch('_centrality', value)
    //    ** NOTE: elements do not yet include temp seed and links
    let values = this.calc_centrality(this.elements, 'degree');
    // console.log('2) calc_centrality', values);

    // 3) create ranks by scaleQuantile: 동일개수묶음에 의한 binning
    //    - set: scratch('_rank', index)
    //    - but, seed nodes should be forced to set topRank
    //      => label Nodes 들은 강제로 최상위 rank 를 가지도록 조정되어야 함
    let ranks = this.set_quanitle(this.elements.nodes(), values);     // array of nodes
    ranks = ranks.filter(x=>x);           // update to not-empty array

    this.topRank = this.set_ranks(ranks, this.seeds);
    // console.log('3) set_quanitle', this.topRank, ranks);

    // 4) make bins as array of ranks by each component : [ component0[ rank0, rank1, ... ], component1, ... ]
    //    - just composed of nodes
    this.bins = this.make_bins(this.components, ranks, this.seeds);
    // console.log('4) make_bins', this.bins);

    // this.seeds = this.get_seeds_by_centrality(this.components);
    // this.exclude_seeds_from_ranks(this.ranks, this.seeds);

    // set style to highest rank
    for( let bin of this.bins ){
      if( bin[this.topRank] ) bin[this.topRank].forEach(e=>e.addClass('seed-semi'));
      bin[this.topRank+1].forEach(e=>e.addClass('seed'));
    }

    // for DEBUG
    window['gc'] = { topRank: this.topRank, bins: this.bins, components: this.components, singles: this.singles };
  }

  getTopRank(){
    return this.topRank;
  }

  doClear(){
    this.bins = [];
    this.components = [];
    this.seeds = [];

    // remove temp_seeds and appended edges
    this.cy.edges('.gclink').remove();
    this.cy.nodes('.seed').remove();
    // remove classes : seed-semi, seed
    this.elements.nodes('.seed-semi').removeClass('seed-semi');
    this.elements.nodes('.seed').removeClass('seed');
    this.elements.nodes('.gcunit').removeClass('gcunit');
    this.elements.style('display','element');
    this.elements = undefined;
    // show all as initial state
    this.singles.style('display','element');
    this.singles = undefined;
    // reset cy
    this.cy = undefined;
  }

  doContraction(currRank:number){
    let aggLinks = [];
    // each component
    for( let i=0; i<this.components.length; i+=1 ){
      let component = this.components[i];
      if( this.bins[i][currRank].size() == 0 ) continue;    // WARN: sometimes happen!

      // targets : collection of nodes
      this.bins[i][currRank].forEach(e=>{
        // currRank == ranks.length-1(최상위) 이면, 같은 components 의 seed 로 연결
        let higher = this.getHigherNeighbor(component, e);
        if( !higher ){    // null => not found
          // for DEBUG
          if( localStorage.getItem('debug')=='true' ) console.log('WARN: cannot find higher neighbor than itself', currRank, e.id(), e.connectedEdges().size(), e);

          higher = this.bins[i][this.topRank];
        }

        let connectedEdges = e.connectedEdges().filter(x=>x.visible());
        this.contractionOnePair(higher, e, connectedEdges);
      });

      // make currRank to in-visible style
      this.bins[i][currRank].forEach(e=>{
        e.connectedEdges().style('display','none');
        e.style('display','none');
      });

      // currRank > 0 이면, edge 제거로 인한 orphans 발생
      // ==> 상위 ranker 에 대한 dijkstra({root:'#id', directed:false}) 로
      //     distanceTo( target ) 최소값을 갖는 target 에 대해 edge 생성 (line-style: dotted or dashed)
      if( currRank-1 < this.topRank ){
        let links = this.scan_orphans_by_contraction(this.components, i, currRank+1);
        if( !links ) links = [];
        // orphans 처리하고도 남은 조각들 연결하기 (order by size desc)
        // **NOTE: links 에 의해 component 가 갱신되었음 ==> this.components[i] 로 접근해야 맞음
        let fragments = this.components[i].filter(x=>x.visible()).components().sort((a,b)=>b.size()-a.size());
        let seedIndex = 0;
        for( let fragment of fragments ){
          if( fragment.contains(this.bins[i][this.topRank]) ){ break; }
          seedIndex += 1;
        }
        if( fragments.length > 1 ){
          let bigger = fragments[seedIndex];
          for( let k=0; k<fragments.length; k+=1 ){
            if( k == seedIndex ) continue;
            let link = this.connect_fragment(this.components, i, currRank+1, bigger, fragments[k]);
            if( link ) links.push(link);
          }
        }
        if( links.length > 0 ) aggLinks = aggLinks.concat(links);
      }
    }
    if( aggLinks.length > 0 ){
      // for DEBUG
      if( localStorage.getItem('debug')=='true' ) console.log('create gclinks:', aggLinks.length);
    }
  }

  doExpansion(currRank:number){
    // each component
    for( let i=0; i<this.components.length; i+=1 ){
      let component = this.components[i];
      if( this.bins[i][currRank].size() == 0 ) continue;    // WARN: sometimes happen!

      // targets : collection of nodes
      // show nodes
      let nodes = this.bins[i][currRank];
      nodes.style('display','element');
      // show edges connected with nodes
      for( let j=currRank+1; j<=this.topRank; j+=1 ) nodes = nodes.union( this.bins[i][j] );
      nodes.connectedEdges(':hidden').style('display','element');

      // remove higher links
      let links = this.cy.edges('.gclink').filter(e=>e.scratch('_rank')<=this.topRank && e.scratch('_rank')>currRank).remove();
      if( links.size() > 0 ){
        // for DEBUG
        if( localStorage.getItem('debug')=='true' ) console.log('remove gclinks:', links.size() );
      }

      // remove class : gcunit
      component.nodes('.gcunit').forEach(e=>{
        let child_nodes = e.scratch('_child_nodes').filter(x=>x.scratch('_rank')<currRank-1);
        if( child_nodes.size() == 0 ) e.removeClass('gcunit');
      });
    }
  }

  ///////////////////////////////////////////////////////////

  // 완전히 분리된 component 들로 분리
  // ==> array of component
  split_components(elements:any, label:string):any[]{
    // exclude solo elements
    let components = elements.components().filter(x=>x.size()>1);
    let componentOrder = 0;
    for( let i=0; i<components.length; i+=1 ){
      let labelNodesCnt = components[i].nodes().filter(x=>x.data('label')==label).size();
      // check component whether having label nodes
      if( labelNodesCnt==0 ){
        this.singles = this.singles.union(components[i]);
        this.elements = this.elements.difference(components[i]);
        components[i] = null;    // exclude
      }
      else{
        components[i].nodes().forEach(e=>e.scratch('_component', componentOrder));
        componentOrder += 1;
      }
    }
    components = components.filter(x=>x!=null);   // update array

    // component 별로 seed 추출 => 저장
    this.seeds = new Array<any>(componentOrder);
    for( let i=0; i<componentOrder; i+=1 ){
      this.seeds[i] = components[i].nodes().filter(x=>x.data('label')==label);
    }

    return components;
  }

  // method 에 따라 nodes 에 _centrality 값 저장하기
  // ==> 구간화 값범위 구하기 위해 values 반환
  calc_centrality(elements:any, method:string){
    // degree centrality normalized : alpha = default value
    let centrality_fn;
    if( method == 'degree' ) centrality_fn = elements.degreeCentralityNormalized({ alpha: 0, directed: false });
    else if( method == 'pagerank' ) centrality_fn = elements.pageRank({ dampingFactor: 0.75, precision: 0.001 });
    else if( method == 'closeness' ) centrality_fn = elements.closenessCentralityNormalized({ directed: false });
    else if( method == 'betweenness' ) centrality_fn = elements.betweennessCentrality({ directed: false });
    else centrality_fn = (x)=>0.0;

    let nodes = elements.nodes().toArray();
    let values = [];
    for( let e of nodes ){
      let value = 0.0;
      if( method == 'degree' ) value = centrality_fn.degree('#'+e.id());
      else if( method == 'pagerank' ) value = centrality_fn.rank('#'+e.id());
      else if( method == 'closeness' ) value = centrality_fn.closeness('#'+e.id());
      else if( method == 'betweenness' ) value = centrality_fn.betweennessNormalized('#'+e.id());
      values.push( value );
      e.scratch('_centrality', value);
    }
    return values;
  }

  // values 의 구간화 rank 를 nodes 에 저장하기
  set_quanitle(nodes:any, values:number[]){
    let ranks:Array<any[]> = new Array(5);
    let rangeFn = d3.scaleQuantile().domain(values).range([0,1,2,3,4]);   // topRank = 5
    nodes.forEach(e=>{
      let rank = rangeFn(e.scratch('_centrality'));
      if( !ranks[rank] ) ranks[rank] = [];
      ranks[rank].push( e );
    });
    return ranks;
  }

  set_ranks(ranks:Array<any[]>, seeds:any[]){
    // excludes seed-nodes from ranks because they should be topRank
    let seedIds = _.flatten( seeds.map(x=>x.map(y=>y.id())) );   // double array
    for(let i=0; i<ranks.length; i+=1){
      ranks[i] = ranks[i].filter(e=> !seedIds.includes(e.id()) );
    }

    let rankIndex=-1;
    for(let rank of ranks){
      if( !rank || rank.length == 0 ) continue;
      rankIndex += 1;
      rank.forEach(e=>{ e.scratch('_rank', rankIndex); });
    }
    return rankIndex+1;   // topRank
  }

  make_bins(components:any[], ranks:Array<any[]>, seeds:any[]):Array<Array<any>> {
    let bins:any[][] = new Array<Array<any>>(components.length);
    for( let component of components ){
      let nodes = component.nodes();
      let i = nodes.first().scratch('_component');  // component index

      bins[i] = new Array<any>(this.topRank+2);     // +1 for semi-seeds, +2 for seeds
      for( let j=0; j<ranks.length; j+=1 ){         // rank index
        let vids = ranks[j].map(x=>x.id());         // having no seeds (label-nodes)
        // collection of nodes with rank j in component i
        bins[i][j] = nodes.filter(e=>vids.includes(e.id())).sort((a,b)=>{
          return a.scratch('_centrality') - b.scratch('_centrality');     // order by asc
        });
      }
      // move highest centrality node to top-rank
      if( this.seeds[i].size() == 1){
        bins[i][this.topRank+1] = this.seeds[i];
        bins[i][this.topRank+1].scratch('_rank', this.topRank+1);
      }
      else if( this.seeds[i].size() > 1 ){
        bins[i][this.topRank] = this.seeds[i];      // semi-seeds
        bins[i][this.topRank].scratch('_rank', this.topRank);

        let boundingBox = this.seeds[i].boundingBox();
        let seed = this.create_temp_seed(i, this.topRank+1, this.label, this.seeds[i].first().scratch('_color')
        , {x:boundingBox.x1+2*boundingBox.w/3, y:boundingBox.y1+2*boundingBox.h/3});
        bins[i][this.topRank+1] = seed;       // create seed-node
        components[i] = components[i].union(seed);
        this.seeds[i].forEach(e=>{            // create seed-links
          let link = this.create_temp_link(i, this.topRank+1, e, seed);
          components[i] = components[i].union(link);
        });
      }
    }
    return bins;
  }

  create_temp_seed(componentIndex:number, rankIndex:number, label:string, color:any, position:any){
    let seed = this.cy.add({
      group:'nodes',
      data:{ id: this.uuid(), label: label, properties:{} },
      scratch:{
        _centrality: 999,
        _component: componentIndex,
        _rank: rankIndex,
        _color: color,
        _child_nodes: this.cy.collection(),
        _child_edges: this.cy.collection()
      },
      position: position
    });
    seed.style('background-color', color);
    return seed;
  }

  create_temp_link(componentIndex:number, rankIndex:number, source:any, target:any){
    let link = this.cy.add({
      group:'edges',
      data:{ id: this.uuid(), label:'gclink', properties:{}, source: source.id(), target: target.id() },
      scratch:{
        _component: componentIndex,
        _rank: rankIndex,
        _color:[source.scratch('_color'), target.scratch('_color')]
      },
      classes:[ 'gclink' ]
    });
    link.style('target-arrow-color', link.scratch('_color')[1]);
    link.style('line-gradient-stop-colors', link.scratch('_color'));
    return link;
  }

  // currRank 제거로 인해 발생한 모든 고아 nodes 들에 대해 link 회복
  scan_orphans_by_contraction(components:any[], componentIndex:number, rankIndex:number){
    let component = components[componentIndex];
    let orphans = component.nodes(':visible').filter(e=>e.connectedEdges(':visible').size()==0);
    if( orphans.size()==0 ) return;
    // make link about orphan nodes
    let links = [];
    orphans.forEach(e=>{
      let selfRank = e.scratch('_rank');
      let restNodes = component.nodes(':visible').filter(x=>x.scratch('_rank')>selfRank).sort((a,b)=>{
        return b.scratch('_centrality') - a.scratch('_centrality');   // order by desc
      });
      if( restNodes.size()==0 ) return false;
      let higher = this.make_link_to_higher(component, e, restNodes );
      // create new edge
      if( higher ){
        let link = this.create_temp_link(componentIndex, rankIndex, e, higher);
        component = component.union(link);
        links.push( link );
      }
      else{
        // for DEBUG
        if( localStorage.getItem('debug')=='true' ) console.log('CANNOT make_link_to_higher', rankIndex, e.id(), e.scratch());
      }
    });
    components[componentIndex] = component;   // update with new links
    return links;
  }

  uuid() {
    var d = _.now();
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = (d + _.random(16)) % 16 | 0;
        d = Math.floor(d / 16);
        return (c == 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
  };

  make_link_to_higher(component:any, target:any, highers:any){
    let minDist = 999;
    let higher = undefined;
    let shortestPathFn = component.dijkstra({root:target, directed:false});
    highers.forEach(e=>{
      let dist = shortestPathFn.distanceTo(this.cy.getElementById(e.id()));
      if( minDist > dist ){
        minDist = dist;
        higher = e;
        if( dist <= 2 ) return false;   // must be shortest path
      }
    });
    return higher;
  }

  // 가장 가까운 높은 랭커 이웃 찾기
  // 1) target 의 neighbor 에서 찾기
  // 2) 없으면 neighbor ^n 에서 찾기 (limit = 6)
  // 3) 찾아도 없으면 null 반환
  getHigherNeighbor(component:any, target:any):any{
    let neighbors = target.neighborhood(':visible').sort((a,b)=>{
      return b.scratch('_rank') - a.scratch('_rank');
    });
    if( neighbors.size() == 0 ) return null;

    let higher = target;
    neighbors.forEach(e=>{
      if( e.scratch('_rank') > higher.scratch('_rank') ){
        higher = e;
        return false;   // break of forEach
      }
    });
    // if not exists in neighbors of 1-depth, search neighbors of neighbors
    let maxLoop = 8;    // limit of search loops
    while( higher == target && maxLoop > 0 ){   // not found
      neighbors = neighbors.neighborhood(':visible').sort((a,b)=>{
        return b.scratch('_rank') - a.scratch('_rank');
      });
      neighbors.forEach(e=>{
        if( e.scratch('_rank') > higher.scratch('_rank') ){
          higher = e;
          return false;   // break of forEach
        }
      });
      maxLoop -= 1;
    }
    return higher == target ? null : higher;
  }

  connect_fragment(components:any, componentIndex:number, rankIndex:number, bigger:any, fragment:any){
    if( !bigger || !fragment ) return;
    if( bigger.nodes().size() == 0 || fragment.nodes().size() == 0 ) return;

    let component = components[componentIndex];
    let target = fragment.nodes().sort((a,b)=>a.scratch('_centrality')-b.scratch('_centrality')).last();
    let highers = bigger.nodes().sort((a,b)=>b.scratch('_centrality')-a.scratch('_centrality'));

    let link = undefined;
    let minDist = 999;
    let higher = undefined;
    let shortestPathFn = component.dijkstra({root:target, directed:false});
    highers.forEach(e=>{
      let dist = shortestPathFn.distanceTo(this.cy.getElementById(e.id()));
      if( minDist > dist ){
        minDist = dist;
        higher = e;
        if( dist <= 2 ) return false;   // must be shortest path
      }
    });
    if( higher ){
      link = this.create_temp_link(componentIndex, rankIndex, target, higher);
      component = component.union(link);
    }
    else{
      // for DEBUG
      if( localStorage.getItem('debug')=='true' ) console.log('CANNOT connect_fragment', rankIndex, target.id(), target.scratch());
    }
    components[componentIndex] = component;   // update with new links
    return link;
  }

  // target(higher rank) 에게 source(lower rank)와 연결된 edgesWith 감추기 (make invisible)
  // 1) depth=1 인 경우 source 와 edgesWith 감추기
  // 2) depth>1 인 경우 link node 를 target 으로 삼아 감추기
  contractionOnePair(higher:any, target:any, edgesWith:any){
    let nodes = higher.scratch('_child_nodes') ? higher.scratch('_child_nodes') : this.cy.collection();
    if( target.size() > 0 ) higher.scratch('_child_nodes', nodes.union( target ));
    let edges = higher.scratch('_child_edges') ? higher.scratch('_child_edges') : this.cy.collection();
    if( edgesWith.size() > 0 ) higher.scratch('_child_edges', edges.union(edgesWith));
    // set style : gcunit (include child-elements)
    if( !higher.hasClass('seed') ) higher.addClass('gcunit');
  }

}