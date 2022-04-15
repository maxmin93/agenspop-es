import { Component, OnInit, HostListener, Output, EventEmitter, ViewChild, ElementRef, OnDestroy, Input, AfterViewInit } from '@angular/core';
// import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, of, Subject, timer, forkJoin, BehaviorSubject } from 'rxjs';
import { catchError, map, tap, debounceTime  } from 'rxjs/operators';
import * as _ from 'lodash';
import { IElement, IGraph, ILabels, EMPTY_GRAPH, ILabel } from 'src/app/models/agens-graph-types';
import { IEvent } from 'src/app/models/agens-data-types';

declare const ElGrapho:any;
const MARGIN_FACTOR = 4;

@Component({
  selector: 'app-webgl',
  templateUrl: './webgl.component.html',
  styleUrls: ['./webgl.component.css']
})
export class WebglComponent implements OnInit, AfterViewInit, OnDestroy {

  private g:IGraph;           // from ParentComponent
  private el: any = undefined;        // elgrapho.js

  private dispLayouts:string[] = ['Chord','Cluster','ForceDirected','Hairball','RadialTree','Tree'];
  private vids_dic = new Map<string,number>();    // for nodes
  model:any = {
    nodes: [],
    edges: []
  };

  // **NOTE : window resize event
  // https://stackblitz.com/edit/debounce-window-resize
  // **NOTE : HostListener list
  // https://gist.github.com/AlexAegis/8741eb49db8112e6d0c05328d64a5e24

  // **NOTE: debounce 로 events 를 걸러 주어야 하기 때문에 rxjs 필요
  // **NOTE: 사용자 이벤트 전달을 위해 EventEmitter 를 사용하면 안됨
  //      ==> @Output 에 의한 올바른 사용이 아니라서 이벤트가 중복으로 두번씩 날라오게됨
  private uiEventsEmitter = new Subject<any>();
  // **NOTE : When ng build --prod, ERROR happened
  // ==>
  // Directive WebglComponent, Expected 1 arguments, but got 0

  @HostListener('window:resize', ['$event.target.innerWidth','$event.target.innerHeight'])
  onWindowResize(width: number, height:number) {
    this.uiEventsEmitter.next({ type:'resize', data: {width, height} });
  }
  @HostListener('mousewheel', ['$event.wheelDelta'])
  onMouseWheel(delta:number):void {
    this.uiEventsEmitter.next({ type:'mouse-wheel', data: Math.floor(delta/120) });
  }
  @HostListener('dblclick', ['$event'])
  onMouseDblClick($event:any):void {
    this.uiEventsEmitter.next({ type:'mouse-dblclick', data: $event });
  }
  @HostListener('document:keyup.shift')
  onShiftKeyUp():void {
    this.uiEventsEmitter.next({ type:'key-shift', data: false });
  }
  @HostListener('document:keydown.shift')
  onShiftKeyDown():void {
    this.uiEventsEmitter.next({ type:'key-shift', data: true });
  }

  private elPrevEvent:any = { type: undefined, data: undefined };  // 중복 idle 이벤트 제거용

  private graph$ = new BehaviorSubject<IGraph>(EMPTY_GRAPH);
  @Input() set graph(g:IGraph) { this.graph$.next(g); }

  @Output() updatePositionsEmitter = new EventEmitter<any[]>();
  @Output() cropToCyEmitter = new EventEmitter<any>();
  @Output() actionEmitter = new EventEmitter<IEvent>();
  @Output() readyEmitter = new EventEmitter<IEvent>();

  @ViewChild("el", {read: ElementRef, static: false}) divEl: ElementRef;

  // for DEBUG : elapsedTime recoding
  private timeLabel:string = null;

  ngOnInit(){
    // UI events
    this.uiEventsEmitter.asObservable()
        .pipe( debounceTime(500) )
        .subscribe(e => this.uiEventsMapper(e));
  }

  ngAfterViewInit(){
    // Async data
    this.graph$.subscribe(x=>{
      if( !x || !x['datasource'] ) return;    // EMPTY_GRAPH
      this.g = <IGraph>x;
      this.loadGraph(this.g);
    });
    // ** webgl demo test
    // this.demoLoad();   // with HttpClientInMemoryWebApiModule
  }

  ngOnDestroy(){
    if( this.el ){
      this.el.setInteractionMode('select');
      this.el.destroy();
    }
    this.g = EMPTY_GRAPH;
  }

  private handleError<T> (operation = 'operation', result?: T) {
    return (error: any): Observable<T> => {
      // for DEBUG
      if( localStorage.getItem('debug')=='true' ) console.log(`${operation} failed: ${error.message}`);

      // Let the app keep running by returning an empty result.
      return of(result as T);
    };
  }

  loadGraph(g:IGraph){
    // create model.nodes for elgrapho
    this.model.nodes = [];
    for( let e of g.nodes ){
      this.model.nodes.push({
        group: e.scratch._label.idx,   // for label coloring
        id: e.data.id,
        _idx: e.scratch._idx,
        _label: e.data.label,
        _component: e.scratch.hasOwnProperty('_$$component') ? e.scratch['_$$component'] : e.scratch._label.idx
      });
    }
    _.orderBy(this.model.nodes, ['_idx'], ['asc']);   // for verification!!

    // create model.edges for elgrapho
    this.model.edges = [];
    for( let e of g.edges ){
      this.model.edges.push({
        from: (e.scratch._source).scratch['_idx'],    // index of source vertex
        to: (e.scratch._target).scratch['_idx']       // index of target vertex
      });
    }

    // make node positions by elgrapho layout
    // **NOTE: this.model 에 x, y 좌표가 추가된다. layout output == model data
    let model = ElGrapho.layouts.ForceDirected(this.model);  // ElGrapho.layouts
    for( let e of g.nodes ){
      e.scratch['_norm_x'] = model.nodes[e.scratch._idx]['x'];
      e.scratch['_norm_y'] = model.nodes[e.scratch._idx]['y'];
    }

    // STEP4) ready event
    this.readyEmitter.emit(<IEvent>{ type: 'node-labels', data: g.labels.nodes });
    this.readyEmitter.emit(<IEvent>{ type: 'edge-labels', data: g.labels.edges });
    this.readyEmitter.emit(<IEvent>{ type: 'layouts', data: this.dispLayouts });
    this.readyEmitter.emit(<IEvent>{ type: 'unre-disable', data: undefined });

    this.elInit(model);
  }

/*
  constructor(private http: HttpClient) { }

  demoLoad(){
    let nodes$:Observable<any[]> = this.http.get<any[]>('api/nodes')
      .pipe( // tap(_ => console.log('fetched nodes of GRAPH')),
        catchError(this.handleError<any[]>('getGraphNodes', []))
      );
    let links$:Observable<any[]> = this.http.get<any[]>('api/links')
      .pipe( // tap(_ => console.log('fetched nodes of GRAPH')),
        catchError(this.handleError<any[]>('getGraphEdges', []))
      );

    // **NOTE: multiple http request with rxjs
    // https://coryrylan.com/blog/angular-multiple-http-requests-with-rxjs
    this.model.nodes = [];
    this.model.edges = [];
    forkJoin([nodes$, links$]).subscribe(results => {
      console.log('  - nodes = '+results[0].length);
      console.log('  - edges = '+results[1].length);
      for( let i=0; i<results[0].length; i+=1){
        let e = results[0][i];
        this.model.nodes.push({ group: e.group, id: 'v'+i, _label: e.name });
      }
      for( let i=0; i<results[1].length; i+=1){
        let e = results[1][i];
        this.model.edges.push({ from: e.source, to: e.target });
      }
      // STEP3) rendering graph
      let model = ElGrapho.layouts.ForceDirected(this.model);  // ElGrapho.layouts
      // for(let x of model.nodes ){
      //   let e = _.find(nodes, y => y.data.id === x.id)
      //   if( e ){ e.scratch('_norm_x',x['x']); e.scratch('_norm_y',x['y']); }
      // }

      this.elInit(model);
    });
  }
*/

  elInit(model:any){
    if( this.el ){
      this.el.setInteractionMode('select');
      this.el.destroy();
    }

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' && this.timeLabel == null ){
      this.timeLabel = `webgl-ready`;
      console.time(this.timeLabel);
    }

    // create WebGL and save to window variable
    this.el = window['el'] = new ElGrapho({
      container: this.divEl.nativeElement,
      model: model, //    // x: [-1.0 ~ +1.0], y: [-1.0 ~ +1.0]
      width: this.divEl.nativeElement.clientWidth - MARGIN_FACTOR,
      height: this.divEl.nativeElement.clientHeight - MARGIN_FACTOR,
      nodeSize: 1,
      edgeSize: 0.1,
      arrows: true,
      debug: true
    });

    // for DEBUG : elapsedTime recording end
    if( localStorage.getItem('debug')=='true' ){
      console.timeEnd(this.timeLabel);
      console.log(`  => nodes(${model.nodes.length}), edges(${model.edges.length})`);
      this.timeLabel = null;
    }

    // make linking el-events to inner
    this.el.emitter$ = (e)=>this.elEventsMapper(e);

    // **NOTE: elgrapho div 에 z-index 사용시 작동하지 않음 (가려짐)
    this.el.tooltipTemplate = function(index, el) {
      if( !window['el'] ) return;
      let node = window['el'].model.nodes[index];
      el.innerHTML = '['+node._label+'] '+node.id;
    };

    // **NOTE: 딱히 필요 없는듯. touch background ==> ex) reset selection
    this.el.on('idle', function(evt) {
      if( !window['el'] ) return;
      window['el'].emitter$({ type: 'idle', data: evt });
    });

    // **NOTE: callback fn 내에서는 this 가 먹지 않음. global 로 접근해야함
    this.el.on('node-click', function(evt) {
      if( !window['el'] ) return;
      window['el'].emitter$({ type: 'node-click', data: window['el'].model.nodes[evt.dataIndex] });
    });

    this.el.on('nodes-crop', function(evt) {
      if( !window['el'] ) return;
      window['el'].emitter$({ type: 'nodes-crop', data: evt });
    });
  }

  /////////////////////////////////////////////////////////

  elEventsMapper(evt:any){
    if( evt.type === 'node-click' ){
      this.elNodeClick(evt.data);
    }
    else if( evt.type === 'nodes-crop' ){
      this.elNodesCrop(evt.data);
    }
    else if( evt.type === 'idle' ){
      this.elBgIdle(evt.data);
    }
    this.elPrevEvent = evt;
  }

  elNodesCrop(targets:any){
    this.cropToCyEmitter.emit(targets);
  }
  elNodeClick(target:any){
    let e = this.g.nodes.find(x=>x.data.id == target.id);
    this.actionEmitter.emit(<IEvent>{
      type: 'property-show',
      data: e   // { index: 'v', id: target.id }
    });
  }
  elBgIdle(target){
    this.actionEmitter.emit(<IEvent>{
      type: 'property-hide',
      data: this.elPrevEvent.type !== 'idle'
    });
  }

  /////////////////////////////////////////////////////////

  uiEventsMapper(evt:any){
    if(evt.type === 'resize') this.uiWindowResize(evt.data);
    else if(evt.type === 'mouse-wheel') this.uiMouseWheel(evt.data);
    else if(evt.type === 'mouse-dblclick') this.uiMouseDblClick(evt.data);
    else if(evt.type === 'key-shift') this.uiPressShiftKey(evt.data);
    else{
      console.log('uiEventsMapper = '+evt.type, evt);
    }
  }

  uiPressShiftKey(isPressed:boolean){
    // console.log('key-shift', isPressed);
    if( !this.el ) return;
    this.el.interactionMode = isPressed ? 'crop' : 'select';
    this.el.setInteractionMode(this.el.interactionMode);
  }

  uiMouseDblClick(evt){
    // console.log('mouse-dblclick', evt);
    if( !this.el ) return;
    this.el.interactionMode = this.el.interactionMode == 'select' ? 'pan' : 'select';
    this.el.setInteractionMode(this.el.interactionMode);
  }

  uiWindowResize(data:any){
    // console.log('onWindowResize', data.width, data.height);
    if( !this.el ) return;
    //this.el.setSize(e.width, e.height);
    window['el'].setSize(data.width-MARGIN_FACTOR, data.height-MARGIN_FACTOR);
  }

  uiMouseWheel(data:any){
    let ZOOM_FACTOR = 1.25;
    let scope_ratio = Math.pow(ZOOM_FACTOR, Math.abs(data));
    if( !this.el ) return;
    if( data > 0 ){
      //this.el.zoomToPoint(0, 0, ZOOM_FACTOR, ZOOM_FACTOR);
      window['el'].zoomToPoint(0, 0, scope_ratio, scope_ratio);
    }
    else if( data < 0 ){
      //this.el.zoomToPoint(0, 0, 1/ZOOM_FACTOR, 1/ZOOM_FACTOR);
      window['el'].zoomToPoint(0, 0, 1/scope_ratio, 1/scope_ratio);
    }
  }

  /////////////////////////////////////////////////////////

  selectElement(index:string, target:IElement){
    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('webgl::selectElement', index, target);

    if( index == 'v' ){
      let e = this.model.nodes[target.scratch._idx];
      this.elNodeClick(e);
      // 시각적 효과: 다른 라벨은 회색 처리하고, 선택 노드는 검정 테두리 강조
      this.el.selectGroup(target.scratch._label.idx);
      this.el.selectNode(target.scratch._idx);
    }
  }

  selectLabel(index:string, label:any){
    if( !this.el ) return;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('selectLabel("'+index+'"):', label);

    if( index === 'v' ) this.el.selectGroup(label.idx);
    // else{
    //   if( this.edge_labels_dic.has(label.label) ){
    //     let arr:IElement[] = this.edge_labels_dic.get(label.label);
    //     let targets = new Set<number>();
    //     arr.forEach(e => {
    //       targets.add( this.node_ids_dic.get(e.data.source) );
    //       targets.add( this.node_ids_dic.get(e.data.target) );
    //     });
    //     this.el.selectNodes(Array.from(targets.values()));
    //   }
    // }
  }

  changeLayout(name:string){
    if( !this.el ) return;

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' ){
      this.timeLabel = `webgl-layout[${name}]`;
      console.time(this.timeLabel);
    }

    let model;
    try{
      // STEP3) rendering graph
      switch (name) {
        case 'Tree': model = ElGrapho.layouts.Tree(this.model);
            break;
        case 'RadialTree': model = ElGrapho.layouts.RadialTree(this.model);
            break;
        case 'Hairball': model = ElGrapho.layouts.Hairball(this.model);
            break;
        case 'Chord': model = ElGrapho.layouts.Chord(this.model);
            break;
        case 'Cluster': model = ElGrapho.layouts.Cluster(this.model, '_component');
            break;
        default:  // ForceDirected
            model = ElGrapho.layouts.ForceDirected(this.model);
      }

      // for DEBUG : elapsedTime recording end
      //   ==> will stop on elInit()

      this.elInit(model);
    }
    catch(e){
      if( e instanceof Error ){
        console.log('ERROR: changeLayout ==>', e.message);
      }
      else{
        throw e;
      }
    }
    finally {
      this.actionEmitter.emit(<IEvent>{
        type: 'layout-stop',
        data: name
      });
    }
  }

  btnDummy(){
    if( !this.el ) return;
    this.el.reset();
  }
}

/*
// ElGrapho.layouts
ElGrapho.layouts.ForceDirected(model);
ElGrapho.layouts.Tree(model);
ElGrapho.layouts.RadialTree(model);
ElGrapho.layouts.Hairball(model);
ElGrapho.layouts.Chord(model);
ElGrapho.layouts.Cluster(model);
*/