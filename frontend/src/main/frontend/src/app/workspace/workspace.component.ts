import { Component, OnInit, ViewChild, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Observable, of, Subject, timer, forkJoin } from 'rxjs';
import { catchError, map, tap, debounceTime  } from 'rxjs/operators';

import { PropertyComponent } from "./property/property.component";
import { IElement, ILabels, EMPTY_GRAPH, IGraph, ILabel } from '../models/agens-graph-types';
import { ApApiService } from '../services/ap-api.service';
import { WebglComponent } from './webgl/webgl.component';
import { CanvasComponent } from './canvas/canvas.component';
import { NgxSpinnerService } from "ngx-spinner";

import * as _ from 'lodash';
import { PALETTE_DARK, PALETTE_BRIGHT } from '../utils/palette-colors';
import { IEvent, ISearch } from '../models/agens-data-types';
import { HeaderComponent } from './header/header.component';

const SCRIPT_GREMLIN:string =  `g.V().hasLabel('customer').sample(40);
g.V().hasLabel('product').sample(40);
g.V().hasLabel('order').sample(300);`;

@Component({
  selector: 'app-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.css']
})
export class WorkspaceComponent implements OnInit, OnDestroy {

  screenMode:string = 'init';
  undoable:boolean = false;
  redoable:boolean = false;
  gcMode:boolean = false;

  spinnerMsg:string = 'Loading...';

  // collection of elements : nodes, edges
  private g   :IGraph = EMPTY_GRAPH;        // original data
  gEl :IGraph = EMPTY_GRAPH;        // for webgl
  gCy :IGraph = EMPTY_GRAPH;        // for canvas
  gSearch:IGraph = undefined;       // Whether screenMode, gEl or gCy

  // find element by id
  private vids:Map<string,IElement>;
  private eids:Map<string,IElement>;

  @ViewChild('propertyComponent', {static: false}) private propertyComponent: PropertyComponent;
  @ViewChild('headerComponent', {static: false}) private headerComponent: HeaderComponent;
  @ViewChild('webglScreen', {static: false}) private webglScreen: WebglComponent;
  @ViewChild('canvasScreen', {static: false}) private canvasScreen: CanvasComponent;
  // private readonly screens:any = {
  //   'webgl' : this.webglScreen,
  //   'canvas': this.canvasScreen
  // };

  // for display
  dispLabels:ILabels = { nodes: [], edges: [] };
  // 사용 가능한 layout : screenMode 에 따라 변경
  dispLayouts:string[] = [];
  // 'webgl': ['Chord','Cluster','ForceDirected','Hairball','RadialTree','Tree'],
  // 'canvas':['bread-first', 'circle', 'cose', 'cola', 'klay', 'dagre', 'cose-bilkent', 'concentric', 'euler']

  // for DEBUG : elapsedTime recoding
  private timeLabel:string = null;

  constructor(
    private route: ActivatedRoute,
    private apApiService: ApApiService,
    private spinner: NgxSpinnerService,
    private _cd: ChangeDetectorRef        // DANGEROUS!!
  ) { }

  ngOnInit(){
    // parameters of routes
    this.route.paramMap.subscribe(params => {
        // console.log('paramMap:', params.get('id'));
        let ds = params.get('ds');
        console.log('param[datasource] =', ds);
        if( ds ){
            // data of routes
            this.route.data.subscribe(data => {
                if( data.hasOwnProperty('mode') ) localStorage.setItem('init-mode', data['mode'].toString());
                if( data.hasOwnProperty('debug') ) localStorage.setItem('debug', data['debug'].toString());
                // load datasource
                this.loadDatasource( ds );
            });
        }
        else{
            this.apApiService.loadConfig().subscribe(x => {	// callback
                if( x.hasOwnProperty('debug') && x['debug'] ) console.log('** config:', x);
                Object.keys(x).forEach(key=>localStorage.setItem(key,x[key]));    // save value as string
            });
        }
    });
  }

  ngOnDestroy(){
    this.g = window['agens'] = EMPTY_GRAPH;
    if( this.vids ) this.vids.clear();
    if( this.eids ) this.eids.clear();
  }

  changeScreenMode($event){
    // for DEBUG
    if( localStorage.getItem('debug')=='true' )
        console.log(`** screenMode change '${this.screenMode}' to '${$event}'`);

    this.screenMode = $event;
    this.gSearch = this.screenMode == 'webgl' ? this.gEl : this.gCy;
  }

  selectQuery($event:any){
    if( !$event || !$event.hasOwnProperty('datasource') || _.isEmpty($event.datasource) ) return;

    if( $event.type == 'gremlin' ) this.loadQueryByGremlin( $event.datasource, $event.script );
    else if( $event.type == 'cypher' ) this.loadQueryByCypher( $event.datasource, $event.script );
    else this.loadDatasource( $event.datasource );
  }

  actionEvent($event:IEvent){    // { type: show/hide, data: { index, id } }
    // for DEBUG
    if( localStorage.getItem('debug')=='true' ){
      if( $event.type != 'spinner' && $event.type != 'property-hide' )
        console.log('actionEvent on '+this.screenMode+':', $event);
    }

    if( $event.type == 'property-show' ){
      // let target = $event.data.index == 'v' ? this.vids.get($event.data.id) : this.eids.get($event.data.id);
      // if( target ) this.propertyComponent.showPanel(target);
      if( $event.data ) this.propertyComponent.showPanel($event.data);
    }
    else if( $event.type == 'property-hide' ){
      this.propertyComponent.hidePanel();
    }
    else if( $event.type == 'layout-stop'){
      this.spinner.hide();
    }
    else if( $event.type == 'spinner'){
      if( $event.data ){
        this.spinnerMsg = 'Running...';
        this.spinner.show();
      }
      else this.spinner.hide();
    }
    else if( $event.type == 'color'){
      this.changeColor($event);
    }
    else if( $event.type == 'icon'){
      this.changeIcon($event);
    }
  }

  readyEvent($event:IEvent){
    if( $event.type == 'node-labels' ){
      setTimeout(()=>{ this.dispLabels.nodes = [...$event.data]; }, 2);
    }
    else if( $event.type == 'edge-labels' ){
      setTimeout(()=>{ this.dispLabels.edges = [...$event.data]; }, 2);
    }
    else if( $event.type == 'layouts' ){
      setTimeout(()=>{
        this.dispLayouts = [...$event.data];
        this.spinner.hide();
      }, 2);
    }
    else if( $event.type == 'undo-changed' ){
      this.undoable = !($event.data).isUndoStackEmpty();
    }
    else if( $event.type == 'redo-changed' ){
      this.redoable = !($event.data).isRedoStackEmpty();
    }
    else if( $event.type == 'gcmode-changed' ){
      this.gcMode = <boolean>($event.data);
    }
  }

  /////////////////////////////////////////////////////////

  private getLabels(arr:IElement[], meta:any):ILabel[] {
    let grp = _.groupBy(arr, 'data.label');   // { labelName: [eles ...], ... }
    let labels:ILabel[] = _.orderBy( Object.keys(meta).map(k => ({
            idx: undefined, elements: undefined,
            name: k, size: (grp.hasOwnProperty(k) ? grp[k].length : 0), total: meta[k]
          })), ['total'], ['asc']);
    for( let i=0; i<labels.length; i+=1 ){
      labels[i]['idx'] = i;
      labels[i]['elements'] = grp.hasOwnProperty(labels[i].name) ? grp[labels[i].name] : [];
      labels[i]['elements'].forEach(e=>e.scratch['_label'] = <ILabel>labels[i]);
    }
    return labels;
  }

  private setColors(labels:ILabels){
    for( let x of labels.nodes ){
      x['color'] = PALETTE_DARK[x['idx']%PALETTE_DARK.length];      // DARK colors
      x['elements'].forEach(e=>{
        e.scratch['_color'] = x['color'];             // string
      });
    }
    for( let x of labels.edges ){
      x['color'] = PALETTE_BRIGHT[x['idx']%PALETTE_BRIGHT.length];  // no meaning!!
      x['elements'].forEach(e=>{
        e.scratch['_color'] = [                       // string[]
          (e.scratch._source).scratch._label.color,   // source node
          (e.scratch._target).scratch._label.color,   // target node
        ];
      });
    }
  }

  private connectedEdges(edges:IElement[], vids:Map<string,IElement>):IElement[] {
    let connected:IElement[] = [];
    for( let e of edges ){
      if( vids.has(e.data.source) && vids.has(e.data.target) ){
        e.scratch._source = vids.get(e.data.source);
        e.scratch._target = vids.get(e.data.target);
        connected.push( e );
      }
    }
    return connected;
  }

  loadDatasource(datasource:string){
    let labels$:any = this.apApiService.findLabelsByDatasource(datasource);
    let nodes$:Observable<IElement[]> = this.apApiService.findAllByDatasource(datasource, 'v');
    let edges$:Observable<IElement[]> = this.apApiService.findAllByDatasource(datasource, 'e');

    this.spinner.show();

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' ){
      this.timeLabel = `loadDatasource[${datasource}]`;
      console.time(this.timeLabel);
    }

    // **NOTE: multiple http request with rxjs
    // https://coryrylan.com/blog/angular-multiple-http-requests-with-rxjs
    forkJoin([labels$, nodes$, edges$]).subscribe(results => {
      // for DEBUG : elapsedTime recording end
      if( localStorage.getItem('debug')=='true' ){
        console.timeEnd(this.timeLabel);
        console.log(`  => nodes(${(<IElement[]>results[1]).length}), edges(${(<IElement[]>results[2]).length})`);
      }

      // STEP0) make dictionary of nodes, edges
      this.vids = new Map<string,IElement>( (<IElement[]>results[1]).map((e,i)=>{
                  e.scratch['_idx'] = i;    // for elgrapho
                  return [e.data.id, e];
                }) );
      this.eids = new Map<string,IElement>( (<IElement[]>results[2]).map((e,i)=>{
                  return [e.data.id, e];
                }) );

      // STEP1) load nodes and make vids
      this.g = {
        datasource: datasource,
        nodes: <IElement[]>results[1],
        edges: [],
        labels: undefined
      };
      // STEP2) load edges
      this.g.edges = this.connectedEdges(<IElement[]>results[2], this.vids);
      // STEP3) load labels
      this.g.labels = {
        nodes: this.getLabels(this.g.nodes, results[0]['V']),
        edges: this.getLabels(this.g.edges, results[0]['E'])
      };
      // STEP4) set colors with own label
      this.setColors(this.g.labels);
      // for DEBUG
      window['agens'] = this.g;

      // STEP 5) activate target screen
      this.gCy = this.gEl = this.g;
      if( localStorage.getItem('init-mode')=='canvas' ){
        this.changeScreenMode('canvas');
      } else {
        this.changeScreenMode('webgl');
      }
    });
  }

  loadQueryByGremlin( datasource:string, script:string ){
    let stmts = script.split(';').map(x=>x.trim())
                      .filter(x=>x.startsWith('g.')).map(x=>datasource+'_'+x);
    let labels$:any = this.apApiService.findLabelsByDatasource(datasource);

    let queries:Observable<any>[] = [labels$];
    for( let stmt of stmts ){
      // for DEBUG
      if( localStorage.getItem('debug')=='true' ) console.log('gremlin:', stmt);
      queries.push( this.apApiService.gremlinQuery(stmt) );
    }

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' ){
      this.timeLabel = `loadDatasource[${datasource}]`;
      console.time(this.timeLabel);
    }

    forkJoin(queries).subscribe(results => {
      let labels = results[0];
      let nodesSet:Set<IElement> = new Set();
      for( let i=1; i<results.length; i+=1 ){
        for( let ele of <IElement[]>results[i] ) nodesSet.add( ele );
      }
      let nodes:IElement[] = Array.from(nodesSet.values());
      this.apApiService.findEdgesOfVertices(datasource, nodes.map(e=>e.data.id)).subscribe(edges=>{

        // for DEBUG : elapsedTime recording end
        if( localStorage.getItem('debug')=='true' ){
          console.timeEnd(this.timeLabel);
          console.log(`  => nodes(${(<IElement[]>nodes).length}), edges(${(<IElement[]>edges).length})`);
        }

        // STEP0) make dictionary of nodes, edges
        this.vids = new Map<string,IElement>( (<IElement[]>nodes).map((e,i)=>{
          e.scratch['_idx'] = i;    // for elgrapho
          return [e.data.id, e];
        }) );
        this.eids = new Map<string,IElement>( (<IElement[]>edges).map((e,i)=>{
          return [e.data.id, e];
        }) );

        // STEP1) load nodes and make vids
        this.g = {
          datasource: datasource,
          nodes: nodes,
          edges: undefined,
          labels: undefined
        };
        // STEP2) load edges
        this.g.edges = this.connectedEdges(edges, this.vids);
        // STEP3) load labels
        this.g.labels = {
          nodes: this.getLabels(this.g.nodes, labels['V']),
          edges: this.getLabels(this.g.edges, labels['E'])
        };
        // STEP4) set colors with own label
        this.setColors(this.g.labels);
        // for DEBUG
        window['agens'] = this.g;

        // STEP 5) activate target screen
        this.gCy = this.gEl = this.g;
        if( localStorage.getItem('init-mode')=='canvas' ){
          this.changeScreenMode('canvas');
        } else {
          this.changeScreenMode('webgl');
        }
        // console.log('loadQueryByGremlin', this.gEl);
      });
    });
  }

  loadQueryByCypher( datasource:string, script:string ){
    let stmts = script.split(';').map(x=>x.trim());
    let labels$:any = this.apApiService.findLabelsByDatasource(datasource);

    let queries:Observable<any>[] = [labels$];
    for( let stmt of stmts ){
      // for DEBUG
      if( localStorage.getItem('debug')=='true' ) console.log('cypher:', stmt);
      queries.push( this.apApiService.cypherQuery(datasource, stmt) );
    }

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' ){
      this.timeLabel = `loadDatasource[${datasource}]`;
      console.time(this.timeLabel);
    }

    forkJoin(queries).subscribe(results => {
      let labels = results[0];
      let nodesSet:Set<IElement> = new Set();
      for( let i=1; i<results.length; i+=1 ){
        for( let ele of <IElement[]>results[i] ) nodesSet.add( ele );
      }
      let nodes:IElement[] = Array.from(nodesSet.values());
      this.apApiService.findEdgesOfVertices(datasource, nodes.map(e=>e.data.id)).subscribe(edges=>{

        // for DEBUG : elapsedTime recording end
        if( localStorage.getItem('debug')=='true' ){
          console.timeEnd(this.timeLabel);
          console.log(`  => nodes(${(<IElement[]>nodes).length}), edges(${(<IElement[]>edges).length})`);
        }

        // STEP0) make dictionary of nodes, edges
        this.vids = new Map<string,IElement>( (<IElement[]>nodes).map((e,i)=>{
          e.scratch['_idx'] = i;    // for elgrapho
          return [e.data.id, e];
        }) );
        this.eids = new Map<string,IElement>( (<IElement[]>edges).map((e,i)=>{
          return [e.data.id, e];
        }) );

        // STEP1) load nodes and make vids
        this.g = {
          datasource: datasource,
          nodes: nodes,
          edges: undefined,
          labels: undefined
        };
        // STEP2) load edges
        this.g.edges = this.connectedEdges(edges, this.vids);
        // STEP3) load labels
        this.g.labels = {
          nodes: this.getLabels(this.g.nodes, labels['V']),
          edges: this.getLabels(this.g.edges, labels['E'])
        };
        // STEP4) set colors with own label
        this.setColors(this.g.labels);
        // for DEBUG
        window['agens'] = this.g;

        // STEP 5) activate target screen
        this.gCy = this.gEl = this.g;
        if( localStorage.getItem('init-mode')=='canvas' ){
          this.changeScreenMode('canvas');
        } else {
          this.changeScreenMode('webgl');
        }
        // console.log('loadQueryByCypher', this.gEl);
      });
    });
  }

  /////////////////////////////////////////////

  private randomPosition(){
    let MARGIN_FACTOR = 20;
    return {
      x: _.random(MARGIN_FACTOR, document.documentElement.clientWidth - MARGIN_FACTOR),
      y: _.random(MARGIN_FACTOR, document.documentElement.clientHeight - MARGIN_FACTOR)
    };
  }

  private getLabelsCropped(arr:IElement[], labels:ILabel[]):ILabel[] {
    let grp = _.groupBy(arr, 'data.label');   // { labelName: [eles ...], ... }
    return labels.map(x=>{
      return <ILabel>{
        idx: x.idx, name: x.name, total: x.size, color: x.color,
        size: grp.hasOwnProperty(x.name) ? grp[x.name].length : 0,
        elements: grp.hasOwnProperty(x.name) ? grp[x.name] : []
      };
    });
  }

  cropToCyGraph($event){
    if( !$event.hasOwnProperty('nodes') || $event.nodes.length == 0 ) return;
    this.spinnerMsg = 'Loading...';
    this.spinner.show();

    let vids:string[] = $event.nodes.map(x=>x.id);    // [id, ...]
    let nodes:IElement[] = _.chain(this.g.nodes)
        .filter(e=> vids.includes(e.data.id) )
        .map(e=>{
          // set position for preset layout
          let pos = _.find($event.nodes, x=>x.id == e.data.id);
          e['position'] = { x: pos._x, y: pos._y };
          e.scratch['_pos'] = { x: pos._x, y: pos._y };   // copy
          return e;
        }).value();
    let edges:IElement[] = _.filter(this.g.edges, e=>
          vids.includes(e.data.source) && vids.includes(e.data.target)
        );
    let labels:ILabels = {
      nodes: this.getLabelsCropped(nodes, this.g.labels.nodes),
      edges: this.getLabelsCropped(edges, this.g.labels.edges)
    };

    // ** have to sync about child component init completion
    // https://stackoverflow.com/a/41389243/6811653
    this.gCy = <IGraph>{
      datasource: this.g.datasource,
      nodes: nodes,
      edges: edges,
      labels: labels,
      pan: $event['pan']
    };
    this.changeScreenMode('canvas');
  }

  returnToElGraph($event){

  }

  /////////////////////////////////////////////

  changeIcon(event:IEvent){
    if( this.screenMode == 'webgl'){
    }
    else{
      this.canvasScreen.changeIcon(event);
    }
  }

  changeColor(event:IEvent){
    if( this.screenMode == 'webgl'){
    }
    else{
      this.canvasScreen.changeColor(event);
    }
  }

  gcEvent(event:IEvent){
    if( this.screenMode == 'webgl'){
    }
    else{
      this.canvasScreen.gcEvent(event);
    }
  }
  undoRedoEvent(cmd:string){
    if( this.screenMode == 'webgl'){
    }
    else{
      this.canvasScreen.undoRedoEvent(cmd);
    }
  }

  selectSearch(item:ISearch){
    if( this.screenMode == 'webgl'){
      if( item.vtype == 'node-label' )
        this.webglScreen.selectLabel('v', item.value);
      else if( item.vtype == 'node' )
        this.webglScreen.selectElement('v', item.value);
    }
    else{
      if( item.vtype == 'node-label' )
        this.canvasScreen.selectLabel('v', item.value);
      else if( item.vtype == 'edge-label' )
        this.canvasScreen.selectLabel('e', item.value);
      else if( item.vtype == 'node' ){
        this.canvasScreen.selectElement('v', item.value);
        // 원래 여기 있으면 안되는 코드지만, 후일로 미룬다 (canvas 쪽으로 이동할 것)
        this.canvasScreen.cyUnselectedFade();
      }
      else if( item.vtype == 'edge' ){
        this.canvasScreen.selectElement('e', item.value);
        // search 에서 넘어온 것인지 구분해야 해서 더티해짐
        this.canvasScreen.cyUnselectedFade();
      }
    }
  }

  selectLabel(index:string, label:any){
    if( this.screenMode == 'webgl'){
      this.webglScreen.selectLabel(index, label);
    }
    else{
      this.canvasScreen.selectLabel(index, label);
    }
  }

  changeLayout(name:string){
    this.spinnerMsg = 'Running...';
    this.spinner.show();

    setTimeout(()=>{
      if( this.screenMode == 'webgl'){
        this.webglScreen.changeLayout(name);
      }
      else{
        this.canvasScreen.changeLayout(name);
      }
    }, 100);
  }

  btnDummy(){
    if( this.screenMode == 'webgl'){
      this.webglScreen.btnDummy();
    } else{
      this.canvasScreen.btnDummy();
    }
  }
}
