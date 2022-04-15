import * as _ from 'lodash';
import * as d3 from 'd3-scale';

export class GraphContraction {

  private cy: any;                      // cytoscape object
  private singles: any;                 // initial singles
  private elements: any;                // initial all elements
  private components:any[];             // array of component(= nodes & edges)
  private bins:any[][];                 // array of component(= array of nodes with same rank)
  private currRank:number = -1;         // target rank about contraction or expansion
  private topRank:number = 5;           // size of rank => quanitle.range([0,1,2,3,4])

  constructor(cy:any, option:any){      // option: { method, data:{ label, key }}
    this.cy = cy;

    // 0) define elements for graph-contraction
    //    - exlucde components of single node
    this.singles = cy.nodes(':visible').filter(e=>e.connectedEdges(':visible').size()==0);
    this.elements = cy.elements(':visible').difference( this.singles );
    // console.log('0) visible elements', singles, elements);

    // ** prepare binning
    // ---------------------------
    // 1) get components => array of nodes and edges
    //    - set: scratch('_component', index)
    if( option.method == 'centrality' )
      this.components = this.split_components(this.elements);
    else
      this.components = this.split_components_with_label(this.elements, option.data.label);
    // console.log('1) split_components', this.components);

    // make singles in-visible
    this.singles.style('display','none');
    // if method='label-value', use default centrality as degree
    let method = option.method == 'centrality' ? option.data : 'degree';

    // 2) calc centralities with user option
    //    - set: scratch('_centrality', value)
    let values = this.calc_centrality(this.elements, method);
    // console.log('2) calc_centrality', values);

    // 3) create ranks by scaleQuantile: 동일개수묶음에 의한 binning
    //    - set: scratch('_rank', index)
    let ranks = this.set_quanitle(this.elements.nodes(), values);    // array of nodes
    this.topRank = this.set_ranks(ranks);
    ranks = ranks.filter(x=>x);   // update to not-empty array
    this.currRank = -1;           // reset currRank
    // console.log('3) set_quanitle', this.topRank, ranks);

    // 4) make bins as array of ranks by each component : [ component0[ rank0, rank1, ... ], component1, ... ]
    //    - just composed of nodes
    if( option.method == 'centrality' )
      this.bins = this.make_bins_by_centrality(this.components, ranks);
    else
      this.bins = this.make_bins_by_labelValue(this.components, ranks, option.data);
    // console.log('4) make_bins', this.bins);

    // this.seeds = this.get_seeds_by_centrality(this.components);
    // this.exclude_seeds_from_ranks(this.ranks, this.seeds);

    // set style to highest rank
    for( let bin of this.bins ){
      bin[ranks.length-1].forEach(e=>e.addClass('seed-semi'));
      bin[ranks.length].forEach(e=>e.addClass('seed'));
    }

    // for DEBUG
    window['gc'] = { topRank: this.topRank, currRank: this.currRank, bins: this.bins, components: this.components, singles: this.singles };
  }

  clear(){
    this.bins = [];
    this.components = [];

    // remove classes : seed-semi, seed
    this.elements.style('display','element');
    this.elements.nodes('.seed-semi').removeClass('seed-semi');
    this.elements.nodes('.seed').removeClass('seed');
    this.elements.nodes('.gcunit').removeClass('gcunit');
    this.elements = undefined;
    // show all as initial state
    this.singles.style('display','element');
    this.singles = undefined;
    // remove appended edges
    this.cy.edges('.gclink').remove();
    this.cy = undefined;
  }

  ////////////////////////////////////////////////////////////////

  // 완전히 분리된 component 들로 분리
  // ==> array of component
  split_components(elements:any):any[]{
    // exclude solo elements
    let components = elements.components().filter(x=>x.size()>1);
    for( let i=0; i<components.length; i+=1 ){
      components[i].nodes().forEach(e=>e.scratch('_component', i));
    }
    return components;
  }

  // component 마다 1) seed 생성, 2) label 과 seed 를 연결하는 edges 생성
  //    - label 은 topRank-1 에 위치, ex) label=customer => 모든 customer
  //    - seed 는 topRank 에 위치, ex) 단 하나의 customer, 또는 label=customer & key=country
  // **NOTE: method='label-value' 인 경우에는 label 이 없는 component 제외
  split_components_with_label(elements:any, label:string, values:string[]=undefined):any[]{
    // exclude solo elements
    let components = elements.components().filter(x=>x.size()>1);
    for( let i=0; i<components.length; i+=1 ){
      // check component whether having label nodes
      if( components[i].nodes().filter(x=>x.data('label')==label).size()==0 ){
        this.singles = this.singles.union(components[i]);
        components[i] = null;   // exclude
      }
    }
    components = components.filter(x=>x!=null);   // update array
    for( let i=0; i<components.length; i+=1 ){
      components[i].nodes().forEach(e=>e.scratch('_component', i));
      // let semi_seeds = components[i].nodes().filter(e=>e.data('label')==label);
      // let seed = this.create_temp_seed(c, r, label, color);
    }
    return components;
  }

  create_temp_seed(componentIndex:number, rankIndex:number, label:string, color:any){
    let seed = this.cy.add({
      group:'edges',
      data:{ id: this.uuid(), label: label },
      scratch:{
        _component: componentIndex,
        _rank: rankIndex,
        _color: color
      }
    });
    seed.style('background-color', color);
    return seed;
  }

  create_temp_link(componentIndex:number, rankIndex:number, source:any, target:any){
    let link = this.cy.add({
      group:'edges',
      data:{ id: this.uuid(), source: source.id(), target: target.id() },
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

  set_ranks(ranks:any[]){
    let rankIndex=-1;
    for(let rank of ranks){
      if( !rank ) continue;   // if empty rank
      rankIndex += 1;
      rank.forEach(e=>{
        e.scratch('_rank', rankIndex);
        e.scratch('_child_nodes', this.cy.collection());   // reset inner nodes (empty collection)
        e.scratch('_child_edges', this.cy.collection());   // reset inner edges (empty collection)
      });
    }
    return rankIndex+1;   // length of ranks
  }

  make_bins_by_centrality(components:any[], ranks:any[]):Array<Array<any>> {
    let bins:any[][] = new Array<Array<any>>(components.length);
    for( let i=0; i<components.length; i+=1 ){
      let nodes = components[i].nodes();

      bins[i] = new Array<any>(this.topRank+1);
      let lastRank = 0;
      for( let j=0; j<ranks.length; j+=1 ){         // rank index
        let vids = ranks[j].map(x=>x.id());
        // collection of nodes with rank j in component i
        bins[i][j] = nodes.filter(e=>vids.includes(e.id())).sort((a,b)=>{
          return a.scratch('_centrality') - b.scratch('_centrality');     // order by asc
        });
        if( bins[i][j].size() > 0 ) lastRank = j;
      }
      // move highest centrality node to top-rank
      bins[i][this.topRank] = bins[i][lastRank].last();
      bins[i][this.topRank].scratch('_rank', this.topRank);
      bins[i][lastRank] = bins[i][lastRank].difference(bins[i][this.topRank]);
    }
    return bins;
  }

  make_bins_by_labelValue(components:any[], ranks:any[], option:any):Array<Array<any>> {
    let bins:any[][] = new Array<Array<any>>(components.length);
    for( let i=0; i<components.length; i+=1 ){
      let nodes = components[i].nodes();
      let label = option.label;   // ==> exclude from rank, and push to topRank forcibly

      bins[i] = new Array<any>(this.topRank+1+1);
      let lastRank = 0;
      for( let j=0; j<ranks.length; j+=1 ){         // rank index
        let vids = ranks[j].filter(x=>x.data('label')!=label).map(x=>x.id());
        // collection of nodes with rank j in component i
        bins[i][j] = nodes.filter(e=>vids.includes(e.id())).sort((a,b)=>{
          return a.scratch('_centrality') - b.scratch('_centrality');     // order by asc
        });
        if( bins[i][j].size() > 0 ) lastRank = j;
      }
      // move node with label to top-rank forcibly
      bins[i][this.topRank] = nodes.filter(x=>x.data('label')==label);
      bins[i][this.topRank].scratch('_rank', this.topRank);
      bins[i][lastRank] = bins[i][lastRank].difference(bins[i][this.topRank]);
    }
    return bins;
  }

  create_temp_seeds(){

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
        // console.log('scan_orphans_by_contraction('+rankIndex+')', e.id(), higher.id(), link);
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
    let component = components[componentIndex];
    if( bigger.nodes().size() == 0 || fragment.nodes().size() == 0 ) return;
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
      // console.log('connect_fragment('+rankIndex+')['+bigger.nodes().size()+']->['+fragment.nodes().size()+']', target.id(), higher.id(), link);
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
    let nodes = higher.scratch('_child_nodes');
    if( target.size() > 0 ) higher.scratch('_child_nodes', nodes.union( target ));
    let edges = higher.scratch('_child_edges');
    if( edgesWith.size() > 0 ) higher.scratch('_child_edges', edges.union(edgesWith));
    // set style : gcunit (include child-elements)
    if( !higher.hasClass('seed') ) higher.addClass('gcunit');
  }

  contraction(){
    if( this.currRank == this.topRank-1 ) return;
    this.currRank += 1;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('graph::contraction', this.currRank);

    let aggLinks = [];
    // each component
    for( let i=0; i<this.components.length; i+=1 ){
      let component = this.components[i];
      if( this.bins[i][this.currRank].size() == 0 ) continue;    // WARN: sometimes happen!

      // targets : collection of nodes
      this.bins[i][this.currRank].forEach(e=>{
        // currRank == ranks.length-1(최상위) 이면, 같은 components 의 seed 로 연결
        let higher = this.getHigherNeighbor(component, e);
        if( !higher ){    // null => not found
          // for DEBUG
          if( localStorage.getItem('debug')=='true' ) console.log('WARN: cannot find higher neighbor than itself', this.currRank, e.id(), e.connectedEdges().size(), e);

          higher = this.bins[i][this.topRank];
        }

        let connectedEdges = e.connectedEdges().filter(x=>x.visible());
        this.contractionOnePair(higher, e, connectedEdges);
      });

      // make currRank to in-visible style
      this.bins[i][this.currRank].forEach(e=>{
        e.connectedEdges().style('display','none');
        e.style('display','none');
      });

      // currRank > 0 이면, edge 제거로 인한 orphans 발생
      // ==> 상위 ranker 에 대한 dijkstra({root:'#id', directed:false}) 로
      //     distanceTo( target ) 최소값을 갖는 target 에 대해 edge 생성 (line-style: dotted or dashed)
      if( this.currRank-1 < this.topRank ){
        let links = this.scan_orphans_by_contraction(this.components, i, this.currRank+1);
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
            let link = this.connect_fragment(this.components, i, this.currRank+1, bigger, fragments[k]);
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

  expansion(){
    if( this.currRank < 0 ) return;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('graph::expansion', this.currRank);

    // each component
    for( let i=0; i<this.components.length; i+=1 ){
      let component = this.components[i];
      if( this.bins[i][this.currRank].size() == 0 ) continue;    // WARN: sometimes happen!

      // targets : collection of nodes
      // show nodes
      let nodes = this.bins[i][this.currRank];
      nodes.style('display','element');
      // show edges connected with nodes
      for( let j=this.currRank+1; j<=this.topRank; j+=1 ) nodes = nodes.union( this.bins[i][j] );
      nodes.connectedEdges(':hidden').style('display','element');

      // remove higher links
      let links = this.cy.edges('.gclink').filter(e=>e.scratch('_rank')>this.currRank).remove();
      if( links.size() > 0 ){
        // for DEBUG
        if( localStorage.getItem('debug')=='true' ) console.log('remove gclinks:', links.size() );
      }

      // remove class : gcunit
      component.nodes('.gcunit').forEach(e=>{
        let child_nodes = e.scratch('_child_nodes').filter(x=>x.scratch('_rank')<this.currRank-1);
        // console.log('remove class: gcunit', child_nodes.size(), e.scratch('_child_nodes').map(x=>x.scratch('_rank')), e);
        if( child_nodes.size() == 0 ) e.removeClass('gcunit');
      });
    }

    this.currRank -= 1;
  }

};
