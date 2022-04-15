import { Component, OnInit, ViewChild, ElementRef, HostListener, Output, EventEmitter, Input, OnDestroy, AfterViewInit, ChangeDetectorRef, TemplateRef } from '@angular/core';
import { Subject, BehaviorSubject, forkJoin } from 'rxjs';
import { debounceTime } from 'rxjs/operators';

import { IElement, IGraph, EMPTY_GRAPH, ILabels, ILabel } from 'src/app/models/agens-graph-types';
import { CY_STYLES, CY_EVT_INIT } from 'src/app/utils/cy-styles';
import { ApApiService } from 'src/app/services/ap-api.service';
import { IEvent } from 'src/app/models/agens-data-types';

import { NgbModal, ModalDismissReasons, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ContextMenuService, ContextMenuComponent } from 'ngx-contextmenu';
import * as _ from 'lodash';

import { GraphContraction } from 'src/app/workspace/canvas/components/graph-contraction';
import { ICON_PREFIX } from 'src/app/utils/palette-colors';

import { GcModalComponent } from './components/gcmodal/gcmodal.component';
import { GcFocusComponent } from './components/gcfocus/gcfocus.component';
import { GcRunService } from 'src/app/services/graph-contraction/gc-run.service';

declare const cytoscape:any;
declare const tippy:any;
declare const jQuery:any;

const MARGIN_FACTOR:number = 4;
const layoutPadding:number = 150;
const aniDur:number = 500;
const easing:string = 'linear';

const CY_CONFIG:any ={
  layout: { name: "preset"
    , fit: false, padding: 100, randomize: false, animate: false, positions: undefined
    , zoom: undefined, pan: undefined, ready: undefined, stop: undefined
  },
  // initial viewport state:
  zoom: 1,
  minZoom: 1e-2,
  maxZoom: 1e1,
  wheelSensitivity: 0.2,
  boxSelectionEnabled: true,
  motionBlur: true,
  selectionType: "single",
  // autoungrabify: true        // cannot move node by user control
}
const UR_CONFIG:any ={
  isDebug: false,
  actions: {},
  undoableDrag: true,
  stackSizeLimit: undefined,
  ready: function () { // callback when undo-redo is ready
  }
}

export interface ICtxMenuItem {
  label: string;
  click: Function;
  subActions?: ICtxMenuItem[];
};

// ** 참고
// https://github.com/cytoscape/wineandcheesemap/blob/gh-pages/demo.js

@Component({
  selector: 'app-canvas',
  templateUrl: './canvas.component.html',
  styleUrls: ['./canvas.component.css']
})
export class CanvasComponent implements OnInit, AfterViewInit, OnDestroy {

  private g:IGraph = undefined;
  cy: any = undefined;                  // cytoscape.js
  ur: any = undefined;                  // cy undoRedo
  gc: any = undefined;                  // graph-contraction

  // vids_dic = new Map<string,number>();    // vid

  private lastHighlighted:any = null;
  private lastUnhighlighted:any = null;

  private cyPrevEvent:IEvent = { type: undefined, data: undefined };  // 중복 idle 이벤트 제거용
  private dispLayouts:string[] = [ 'breadthfirst', 'circle', 'cose', 'cola', 'klay', 'dagre', 'cose-bilkent', 'concentric', 'euler' ];

  // for doubleTap
  tappedBefore:any;
  tappedTimeout:any;
  tappedTarget:any;
  tappedCount:number = 0;

  // ctxmenu example
  ctxUserActions:ICtxMenuItem[] = [];
  ctxMenuActions:any = {
    notExistSelect: ()=>{
      return this.cy && this.cy.$(':selected').size() == 0;
    },
    existSelect: ()=>{
      return this.cy && this.cy.$(':selected').size() > 0;
    },
    selectWithType: (cy:any, type:string)=>{
      if( !cy ) return;
      cy.$(':selected').unselect();

      let eles;
      if( type == 'leaf' ){ eles = cy.nodes().leaves(); }
      else if( type == 'orphan' ){ eles = cy.nodes().filter(e=>e.connectedEdges().size()==0); }
      else{ eles = cy.nodes(); }

      Promise.resolve().then(()=>{
        eles.grabify();
        eles.select();
      });
    },
    invertSelect: (cy:any)=>{
      let eles = cy.$(':selected');
      if( eles.size() == 0 ) return;
      Promise.resolve().then(()=>{
        cy.$('node:unselected').grabify();
        cy.$('node:unselected').select();
      }).then(()=>{
        eles.ungrabify();
        eles.unselect();
      });
    },
    isolateSelect: (cy:any)=>{
      let eles = cy.$(':selected');
      if( eles.size() == 0 ) return;
      Promise.resolve().then(()=>{
        this.ur.do('remove', cy.$('node:unselected'));  // cy.remove( cy.$('node:unselected') );
      }).then(()=>{
        eles.unselect();
      });
    },
    removeSelect: (cy:any)=>{
      let eles = cy.$(':selected');
      if( eles.size() == 0 ) return;
      this.ur.do('remove', eles);    // cy.remove(eles);
    },
    toggleCaption: (cy:any, select:string)=>{
      if( select == 'id' ) cy.nodes().addClass('captionId');
      else if( select == 'label' ) cy.nodes().addClass('captionLabel');
      else if( select == 'name' ) cy.nodes().addClass('captionName');
      else{
        cy.nodes().removeClass('captionId');
        cy.nodes().removeClass('captionLabel');
        cy.nodes().removeClass('captionName');
      }
    }
  };
  @ViewChild('cyMenu', {static: false}) public cyMenu: ContextMenuComponent;
  @ViewChild('cyBgMenu', {static: false}) public cyBgMenu: ContextMenuComponent;

  private uiEventsEmitter = new Subject<any>();
  @HostListener('window:resize', ['$event.target.innerWidth','$event.target.innerHeight'])
  onWindowResize(width: number, height:number) {
    this.uiEventsEmitter.next( {type: 'resize', data: {width, height}} );
  }
  @HostListener('mousewheel', ['$event.wheelDelta'])
  onMouseWheel(delta:number): void {
    this.uiEventsEmitter.next( {type: 'mouse-wheel', data: Math.floor(delta/120)} );
  }
  // @HostListener('dblclick', ['$event'])
  // onMouseDblClick($event:any): void {
  //   this.uiEventsEmitter.next({ type:'mouse-dblclick', data: $event });
  // }

  private graph$ = new BehaviorSubject<IGraph>(EMPTY_GRAPH);
  @Input() set graph(g:IGraph) { this.graph$.next(g); }

  @Output() returnToElEmitter= new EventEmitter<any>();
  @Output() actionEmitter= new EventEmitter<IEvent>();
  @Output() readyEmitter= new EventEmitter<IEvent>();

  @ViewChild("cy", {read: ElementRef, static: false}) divCy: ElementRef;

  private gcOption:any;

  // for DEBUG : elapsedTime recoding
  private timeLabel:string = null;

  constructor(
    private cdr: ChangeDetectorRef,
    private apApiService: ApApiService,
    private modalService: NgbModal,
    private contextMenuService: ContextMenuService
  ) {
    // UI events
    this.uiEventsEmitter.asObservable()
        .pipe( debounceTime(500) )
        .subscribe(e => this.uiEventsMapper(e));
  }

  ngOnInit(){
    // UI events
    this.uiEventsEmitter.asObservable()
        .pipe( debounceTime(500) )
        .subscribe(e => this.uiEventsMapper(e));
  }

  ngAfterViewInit(){
    // Async data (ex: crop to cy-graph)
    this.graph$.subscribe(x => {
      if( !x || !x['datasource'] ) return;
      this.g = <IGraph>x;
      this.loadGraph(x);
    });
  }

  ngOnDestroy(){
    // https://atomiks.github.io/tippyjs/methods/#destroy
    if( this.cy ){
      this.cy.nodes().forEach(e=>{
        if( e.scratch('_tippy') ) e.scratch('_tippy').destroy();
      });
      if( !this.cy.destroyed() ) this.cy.destroy();
    }

    this.cy = window['cy'] = undefined;
    this.g = EMPTY_GRAPH;
  }

  private setStyleNode(e:any){
    e.ungrabify();
    if( e.scratch('_color') ){
      e.style('background-color', e.scratch('_color'));
    }

    if( e.scratch('_icon') ){
      e.style('background-image', ICON_PREFIX+e.scratch('_icon').path);
      e.style('border-opacity', 0);
      e.addClass('icon');
    }
    else{
      e.style('background-image', null);
      e.style('border-opacity', 1);
      e.removeClass('icon');
    }
  }
  private setStyleEdge(e:any){
    if( e.scratch('_color') && e.scratch('_color').length == 2 ){
      e.style('target-arrow-color', e.scratch('_color')[1]);
      e.style('line-gradient-stop-colors', e.scratch('_color'));
    }
  }

  loadGraph(g:IGraph){
    // for DEBUG
    // if( localStorage.getItem('debug')=='true' ) console.log('loadGraph', g);

    let pan = g.hasOwnProperty('pan') ? g['pan'] : { x:0, y:0 };
    let config:any = Object.assign( _.cloneDeep(CY_CONFIG), {
      container: this.divCy.nativeElement,
      elements: _.concat(g.nodes, g.edges),
      style: CY_STYLES,
      pan: pan,
      ready: (e)=>{
        let cy = e.cy;
        cy.scratch('_datasource', g.datasource);
        cy.nodes().forEach(e => this.setStyleNode(e));
        cy.edges().forEach(e => this.setStyleEdge(e));
      }
    });

    // STEP4) ready event
    this.readyEmitter.emit(<IEvent>{ type: 'node-labels', data: g.labels.nodes });
    this.readyEmitter.emit(<IEvent>{ type: 'edge-labels', data: g.labels.edges });
    this.readyEmitter.emit(<IEvent>{ type: 'layouts', data: this.dispLayouts });

    this.cyInit(config);
  }

  cyInit(config:any){
    cytoscape.warnings(false);                 // ** for PRODUCT : custom wheel sensitive

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' ){
      this.timeLabel = `canvas-ready`;
      console.time(this.timeLabel);
    }

    if( localStorage.getItem('init-mode')=='canvas' ){
        // 최초 layout: euler
        config.layout = { name: "euler"
        , fit: true, padding: 100, randomize: true, animate: false, positions: undefined
        , zoom: undefined, pan: undefined, ready: undefined
        , stop: function(event){
            console.log('init-layoutstop: nodes.size =', event.target._private.cy.nodes().size());
            // backup position of highlight targets
            event.target._private.cy.nodes().forEach(n=>{
              n.scratch('_pos', n.position());
            });
            event.target.removeAllListeners();
          }
        };
    }
    this.cy = window['cy'] = cytoscape(config);

    // **NOTE : 여기서 측정하는 것이 ready()에서 측정하는 것보다 1초+ 정도 느리다.
    //      ==> ready() 에서 모든 nodes, edges 들의 style 처리후 빠져나옴
    //      ==> 이 시점에서 화면상에 그래프는 보이지 않음. 브라우저에서 실제 그리는 시간이 추가로 소요됨 (측정불가. 도구가 없음)

    // for DEBUG : elapsedTime recording end
    if( localStorage.getItem('debug')=='true' ){
      console.timeEnd(this.timeLabel);
      console.log(`  => nodes(${this.cy.nodes().size()}), edges(${this.cy.edges().size()})`);
      this.timeLabel = null;
    }

    // undo-redo
    this.ur = this.cy.undoRedo(UR_CONFIG);
    this.cy.on("afterDo", (event, actionName, args, res)=>{
      this.readyEmitter.emit(<IEvent>{ type: 'undo-changed', data: this.ur });
    });
    this.cy.on("afterUndo", (event, actionName, args, res)=>{
      this.readyEmitter.emit(<IEvent>{ type: 'undo-changed', data: this.ur });
      this.readyEmitter.emit(<IEvent>{ type: 'redo-changed', data: this.ur });
    });
    this.cy.on("afterRedo", (event, actionName, args, res)=>{
      this.readyEmitter.emit(<IEvent>{ type: 'redo-changed', data: this.ur });
    });


    // make linking el-events to inner
    this.cy._private.emitter.$customFn = (e)=>this.cyEventsMapper(e);
    ///////////////////////////////
    // register event-handlers
    ///////////////////////////////
    let cy = this.cy;

    // right-button click : context-menu on node
    cy.on('cxttap', (e)=>{
      if( e.target === cy ){
        this.contextMenuService.show.next({
          anchorElement: cy.popperRef({renderedPosition: () => ({
            x: e.originalEvent.offsetX-5, y: e.originalEvent.offsetY-5 }),}),
          contextMenu: this.cyBgMenu,
          event: <MouseEvent>e.orignalEvent,
          item: e.target
        });
      }
      // **NOTE: ngx-contextmenu is FOOLISH! ==> do change another!
      else if( e.target.isNode() ){
        this.listVertexNeighbors(e.target, ()=>{
          this.contextMenuService.show.next({
            anchorElement: e.target.popperRef(),
            contextMenu: this.cyMenu,
            event: <MouseEvent>e.orignalEvent,
            item: e.target
          });
        });
      }

      e.preventDefault();
      e.stopPropagation();
    });

    // ** 탭 이벤트를 cyEventsMapper()로 전달
    cy.on('tap', (e)=>{
      let tappedNow = event.target;
      if( this.tappedTimeout && this.tappedBefore) {
        clearTimeout(this.tappedTimeout);
      }
      if( this.tappedBefore === tappedNow ){
        e.target.trigger('doubleTap', e);
        e.originalEvent = undefined;
        this.tappedBefore = null;
      }
      else{
        this.tappedTimeout = setTimeout(()=>{
          if( e.target === cy ){    // click background
            cy._private.emitter.$customFn({ type: 'idle', data: e.target });
          }                         // click node or edge
          else if( e.target.isNode() || e.target.isEdge() ){
            cy._private.emitter.$customFn({ type: 'ele-click', data: e.target });
          }
          this.tappedBefore = null;
        }, 300);
        this.tappedBefore = tappedNow;
      }
    });

    // trigger doubleTap event
    // https://stackoverflow.com/a/44160927/6811653
    cy.on('doubleTap', _.debounce( (e, originalTapEvent) => {
      if( e.target !== cy && e.target.isNode() ){
        cy._private.emitter.$customFn({ type: 'node-dblclick', data: e.target });
      }
    }), 500);

    cy.on('boxselect', _.debounce( (e)=>{
      cy.$(':selected').nodes().grabify();
    }), 500);

    cy.on('dragfree','node', (e)=>{
      let pos = e.target.position();
      e.target.scratch('_pos', _.clone(pos));
    });

    cy.on('select','node', (e)=>{
      e.target.style('background-color', '#fff');
      if( !e.target.hasClass('seed')) e.target.style('border-color', e.target.scratch('_color'));
      e.target.style('border-opacity', 1);
      if( !e.target.hasClass('seed')) e.target.style('z-index', 9);
      if( e.target.scratch('_tippy') ) e.target.scratch('_tippy').hide();
    });

    cy.on('unselect','node', (e)=>{
      e.target.ungrabify();
      e.target.style('background-color', e.target.scratch('_color'));
      if( !e.target.hasClass('seed')) e.target.style('border-color', '#fff');
      if( e.target.hasClass('icon')) e.target.style('border-opacity', 0);
      if( !e.target.hasClass('highlighted') && !e.target.hasClass('seed') ) e.target.style('z-index', 0);
      if( e.target.scratch('_tippy') ) e.target.scratch('_tippy').hide();
    });

    // ** node 선택을 위한 편의 기능 (뭉쳤을때)
    cy.on('mouseover', 'node', _.debounce( (e)=>{
      let node = e.target;
      if( node && !node.selected() ){
        if( !node.hasClass('faded') ){        // show
          if( !node.hasClass('highlighted') && !node.hasClass('seed') ) node.style('z-index', 1);
          // node.scratch('_tippy').show();
          setTimeout(()=>{                    // auto-hide
            if( !node.hasClass('highlighted') && node.hasClass('seed') ) node.style('z-index', 0);
          }, 2000);
        }
      }
    }, 200));

  }

  /////////////////////////////////////////////////////////

  cyEventsMapper(evt:any){
    if( evt.type === 'ele-click' ){
      this.cyElementClick(evt.data);
    }
    else if( evt.type === 'node-dblclick' ){
      this.cyNodeDblClick(evt.data);
    }
    else if( evt.type === 'idle' ){
      this.cyBgIdle(evt.data);
    }
    this.cyPrevEvent = evt;
  }

  cyUnselectedFade(){
    setTimeout(()=>{
      this.cy.elements(':unselected').addClass('faded');
    }, 10);
  }

  cyElementClick(target:any){
    let e = target.size() > 1 ? target.first() : target;
    let json = <IElement>e.json();      // expand 된 개체는 g 모체에 없기 때문에 직접 추출
    json.scratch = e.scratch();         // json() 출력시 scratch 는 누락됨
    this.actionEmitter.emit(<IEvent>{
      type: 'property-show',
      data: json   //{ index: e.isNode() ? 'v' : 'e', id: e.id() }
    });

    if( e.isEdge() ){
      // edge 선택시 연결된 source, target nodes 도 함께 선택
      // ==> mouse drag 가능해짐 (양끝 노드 하나를 붙잡고 이동)
      setTimeout(()=>{
        this.cy.batch(()=>{
          e.source().select();
          e.target().select();
        });
      }, 2);
    }
    // else this.cyHighlight(e);
  }

  cyNodeDblClick(target:any){
    let e = target.size() > 1 ? target.first() : target;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('DblClick('+this.tappedCount+'):', e.id());

    if( this.tappedTarget == e ) this.tappedCount += 1;
    else{
      this.tappedCount = 1;
      this.tappedTarget = e;
    }

    let eles = this.cy.collection().add(e);
    for( let i=0; i<this.tappedCount; i+=1 ){
      eles = eles.union( eles.neighborhood() );
    }

    eles.grabify();
    eles.select();
  }

  cyBgIdle(target){
    this.actionEmitter.emit(<IEvent>{
      type: 'property-hide',
      data: this.cyPrevEvent.type !== 'idle'
    });
    // // reset doubleTap
    if( this.tappedTarget ){
      this.tappedCount = 0;
      this.tappedTarget = null;
    }

    // when label selection, the others set faded
    // ==> release faded style
    this.cy.batch(()=>{   // ==> without triggering redraws
      this.cy.$('node:selected').ungrabify();
      if( !this.lastHighlighted ){
        this.cy.edges().removeClass('faded');
        this.cy.nodes().removeClass('faded');
      }
      this.cy.nodes().filter(e=>!e.selected() && !e.hasClass('highlighted') && !e.hasClass('seed')).style('z-index', 0);
      this.cy.nodes().forEach(e=>{ if(e.scratch('_tippy')) e.scratch('_tippy').hide(); });
    });
    this.cy.$(':selected').unselect();
  }

  cyMakeTippy(node, text){
    return tippy( node.popperRef(), {
      content: function(){
        var div = document.createElement('div');
        div.innerHTML = text;
        return div;
      },
      trigger: 'manual',
      arrow: true,
      placement: 'bottom',
      hideOnClick: false,
      multiple: true,
      sticky: true
    } );
  };

  ///////////////////////////////////////////////////////
  // **REF: https://github.com/cytoscape/wineandcheesemap

  // for context-menu template
  isNode(item: any): boolean {
    return item.isNode();
  }

  private isDirty(){
    return this.lastHighlighted != null;
  }
  private promiseDelay(duration){
    return new Promise<void>(function(resolve, reject){
      setTimeout(function(){ resolve(); }, duration)    // delay
    });
  }
  private restoreElesPositions( nhood ){
    return Promise.all( nhood.map( ele =>{
              return ele.animation({
                position: _.clone(ele.scratch('_pos')),
                duration: aniDur,
                easing: easing
              }).play().promise();
            }) );
  };

  cyUnhighlight( opts=undefined ){
    if( !this.isDirty() ){ return Promise.resolve(); }

    this.cy.stop();
    this.cy.elements().stop();

    let nhood = this.lastHighlighted;
    let others = this.lastUnhighlighted;
    this.lastHighlighted = this.lastUnhighlighted = null;

    let resetHighlight = ()=>{
      nhood.removeClass('highlighted');
      nhood.forEach(e=>{
        if( e.scratch('_tippy') ) e.scratch('_tippy').hide();
      });
    };

    let hideOthers = ()=>{
      return this.promiseDelay( 125 ).then(()=>{
                others.addClass('hidden');
                return this.promiseDelay( 125 );
              });
    };

    let restorePositions = ()=>{
      this.cy.batch(()=>{
        others.nodes().forEach(n=>{
          n.position( _.clone(n.scratch('_pos')) );
        });
      });
      return this.restoreElesPositions( nhood.nodes() );
    };

    let showOthers = ()=>{
      this.cy.batch(()=>{
        this.cy.elements().removeClass('hidden').removeClass('faded');
      });
      return this.promiseDelay( aniDur );
    };

    let resetPanning = ()=>{
      this.cy.animation({
        fit: {
          eles: nhood,
          padding: layoutPadding
        },
        duration: 200,
        easing: easing
      }).play();
    };

    return Promise.resolve()
            .then( ()=>this.actionEmitter.emit(<IEvent>{ type:'spinner', data:true }))
            .then( resetHighlight )
            .then( hideOthers )
            .then( restorePositions )
            .then( showOthers )
            .then( resetPanning )
            .then( ()=>this.actionEmitter.emit(<IEvent>{ type:'spinner', data:false }));
  }

  cyHighlight( node:any ){
    let oldNhood = this.lastHighlighted;
    let nhood = this.lastHighlighted = node.closedNeighborhood();
    let others = this.lastUnhighlighted = this.cy.elements().not( nhood );

    let reset = ()=>{
      this.cy.batch(()=>{
        others.addClass('hidden');
        nhood.removeClass('hidden');
        this.cy.elements().removeClass('faded highlighted');

        nhood.addClass('highlighted');
        // nhood.style('z-index', 99);
        others.nodes().forEach(n=>{
          n.position( _.clone(n.scratch('_pos')) );
        });
      });

      return Promise.resolve().then(()=>{
                if( this.isDirty() ) return fit();
                else return Promise.resolve();
              }).then(()=>{
                return this.promiseDelay(aniDur);    // delay
              });
    };

    let runLayout = ()=>{
      let p = _.clone(node.scratch('_pos'));
      let l = nhood.filter(':visible').makeLayout({
        name: 'concentric', fit: false,
        animate: true, animationDuration: aniDur, animationEasing: easing,
        boundingBox: { x1: p.x - 1, x2: p.x + 1, y1: p.y - 1, y2: p.y + 1 },
        avoidOverlap: true,
        concentric: function( ele ){
          if( ele.same( node ) ){ return 2; } else { return 1; }
        },
        levelWidth: function(){ return 1; },
        padding: layoutPadding
      });
      let promise = this.cy.promiseOn('layoutstop');
      l.run();
      return promise;
    };

    let fit = ()=>{
      return this.cy.animation({
                fit: {
                  eles: nhood.filter(':visible'),
                  padding: layoutPadding
                },
                easing: easing,
                duration: aniDur
              }).play().promise();
    };

    let showOthersFaded = ()=>{
      return this.promiseDelay(250)
                .then(()=>{
                  this.cy.batch(()=>{
                    others.removeClass('hidden').addClass('faded');
                  });
                });
    };

    return Promise.resolve()
            .then( ()=>this.actionEmitter.emit(<IEvent>{ type:'spinner', data:true }))
            .then( reset )
            .then( runLayout )
            .then( fit )
            .then( showOthersFaded )
            .then( ()=>this.actionEmitter.emit(<IEvent>{ type:'spinner', data:false }));
  }

  /////////////////////////////////////////////////////////

  uiEventsMapper(evt:any){
    if(evt.type === 'resize') this.uiWindowResize(evt.data);
    else if(evt.type === 'mouse-wheel') this.uiMouseWheel(evt.data);
    // else if(evt.type === 'mouse-dblclick') this.uiMouseDblClick(evt.data);
    else{
      console.log('uiEventsMapper = '+evt.type, evt);
    }
  }

  uiWindowResize(data:any){
    if( !this.cy ) return;
    this.cy.resize();
  }
  uiMouseWheel(data:any){
    if( !this.cy ) return;
    let ZOOM_FACTOR = 1.25;
    let scope_ratio = Math.pow(ZOOM_FACTOR, Math.abs(data));
  }

  /////////////////////////////////////////////////////////

  showEvent(event:any) {
    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('showEvent', event);
  }

  private expandNeighbors(event:any, target:any, nbrIds: string[]){
    let node = jQuery(event.target).is('small') ? jQuery(event.target) : jQuery(event.target).find('small');
    let selector = node.attr('prefix')+'.'+node.attr('label');

    // 캔버스상의 모든 nodes 에 대한 연결된 edges 가져오기
    let vids = [...nbrIds].concat( this.cy.nodes().map(x=>x.id()) );
    let eids = this.cy.edges().map(x=>x.id());

    let nodes$ = this.apApiService.findByIds(this.g.datasource, 'v', nbrIds);
    let edges$ = this.apApiService.findEdgesOfVertices(this.g.datasource, vids);
    forkJoin([nodes$, edges$]).subscribe(results => {
      // filter new edges except exists edges
      let tmpEdges = (<IElement[]>results[1]).filter(x=>!eids.includes(x.data.id));

      // make invisible before add
      (<IElement[]>results[0]).forEach(x=>x['classes']=['invisible']);
      tmpEdges.forEach(x=>x['classes']=['invisible']);

      // before display, add to cy with invisible
      let nodes = this.cy.add( results[0] );
      let edges = this.cy.add( tmpEdges );
      let elements = nodes.union(edges);
      this.cy.remove( elements );

      // make visible before restore
      elements.removeClass('invisible');
      this.ur.do('restore', elements);

      Promise.resolve()
      .then(()=>{
        nodes.forEach(e =>{
          let label:ILabel = _.find( this.g.labels.nodes, {name: e.data('label')} );
          if( label ){
            e.scratch('_color', label.color);
            this.setStyleNode(e);
          }
        });
        edges.forEach(e =>{
          let srcLabel:ILabel = _.find( this.g.labels.nodes, {name: e.source().data('label')} );
          let dstLabel:ILabel = _.find( this.g.labels.nodes, {name: e.target().data('label')} );
          e.scratch('_color', [srcLabel.color, dstLabel.color]);
          this.setStyleEdge(e);
        });
      })
      .then(()=>{
        let pos = target.position();
        let boundingBox = { x1:pos.x-40, y1:pos.y-40, w:100, h:100 };
        let handle = nodes.union(target).union(edges).layout({
          name: 'concentric', boundingBox: boundingBox,
          fit: false, animate: 'end', animationDuration: 200,
          minNodeSpacing: 20, avoidOverlap: true, avoidOverlapPadding: 20,
          concentric: function(node){ return node.id() == target.id() ? 999 : node.degree(); },
          stop: (l)=>{
            for( let e of nodes.toArray() ){
              e.scratch('_pos', _.clone(e._private.position));
            }}
        });
        handle.run();
      });

      // for DEBUG
      if( localStorage.getItem('debug')=='true' ) console.log('expandNeighbors('+selector+'):', target.id());
    });
  }

  listVertexNeighbors(e:any, callback:Function){
    this.ctxUserActions = [];
    this.apApiService.listVertexNeighbors(this.g.datasource, e.id()).subscribe(x=>{
      // => { outgoers: { label: [vid...], ...}, incomers: { label: [vid...], ...} }
      let excludeVids = e.neighborhood().map(v=>v.id());
      let outgoers = _.map(x.outgoers, (vids, label) => ({
        label: label, prefix: '_outgoers', click:(event,item)=>this.expandNeighbors(event, item, vids),
        vids: vids.filter(v=>!excludeVids.includes(v))
      }));
      let incomers = _.map(x.incomers, (vids, label) => ({
        label: label, prefix: '_incomers', click:(event,item)=>this.expandNeighbors(event, item, vids),
        vids: vids.filter(v=>!excludeVids.includes(v))
      }));
      this.ctxUserActions.push(<ICtxMenuItem>{
        label: 'outgoers', click:(event,item)=>console.log(event,item),
        subActions: outgoers, size: outgoers.reduce((agg,v)=>agg+v.vids.length, 0)
      });
      this.ctxUserActions.push(<ICtxMenuItem>{
        label: 'incomers', click:(event,item)=>console.log(event,item),
        subActions: incomers, size: incomers.reduce((agg,v)=>agg+v.vids.length, 0)
      });

      // console.log('==>', this.ctxUserActions);
      e.scratch('_outgoers', outgoers );
      e.scratch('_outgoers.size', outgoers.length == 0 ? 0 : _.sumBy(outgoers, (x)=>x.vids.length) );
      e.scratch('_incomers', incomers );
      e.scratch('_incomers.size', incomers.length == 0 ? 0 : _.sumBy(incomers, (x)=>x.vids.length) );

      Promise.resolve().then(()=>this.cdr.detectChanges()).then(()=>callback.call(null));
    });
  }

  selectElement(index:string, target:IElement){
    let e = this.cy.getElementById(target.data.id);
    if( e ){
      e.select();
      this.cyElementClick(e);
    }
  }

  selectLabel(index:string, label:ILabel){
    if( !this.cy ) return;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('selectLabel("'+index+'"):', label);

    // this.cy.$(':selected').unselect();
    Promise.resolve()
      .then( ()=>this.cy.$(':selected').unselect())
      .then( ()=>this.cyBgIdle(null) )
      .then( ()=>{
        this.cy.batch(()=>{
          if( index == 'v' ){
            this.cy.nodes().filter(e=>e.data('label') == label.name).select();
            this.cy.nodes().filter(e=>e.data('label') !== label.name).addClass('faded');
          } else{
            let edges = this.cy.edges().filter(e=>e.data('label') == label.name);
            (this.cy.edges().difference( edges)).addClass('faded');                   // faded edges
            (this.cy.nodes().difference( edges.connectedNodes())).addClass('faded');  // faded nodes
          }
        });
      });
  }

  changeLayout(name:string){
    if( !this.cy ) return;

    let eles = this.cy.elements(':visible');
    let boundingBox = undefined;
    if( this.cy.$(':selected').size() > 1 ){
      eles = this.cy.$(':selected');
      boundingBox = eles.boundingBox();
    }

    // for DEBUG : elapsedTime recording start
    if( localStorage.getItem('debug')=='true' ){
      this.timeLabel = `canvas-layout[${name}]`;
      console.time(this.timeLabel);
    }

    // let fit_option = localStorage.getItem('debug')=='true' ? false : true;
    let animate_option = localStorage.getItem('debug')=='true' ? false : 'end';

    let handle = eles.layout({
      name: name, boundingBox: boundingBox,
      fit: true, padding: 100, animate: animate_option, animationDuration: 500,
      minNodeSpacing: 20, avoidOverlap: true, avoidOverlapPadding: 20,
      spacingFactor: 1.5, nodeDimensionsIncludeLabels: false,
      ready: undefined, stop: (l)=>{
        // for DEBUG : elapsedTime recording end
        if( localStorage.getItem('debug')=='true' ){
          console.timeEnd(this.timeLabel);
          console.log(`  => nodes(${eles.nodes().size()}), edges(${eles.edges().size()})`);
          this.timeLabel = null;
        }

        for( let e of eles.toArray() ){
          e.scratch('_pos', _.clone(e._private.position));
        }
        this.actionEmitter.emit(<IEvent>{
          type: 'layout-stop',
          data: name
        });
      }
    });
    handle.run();
  }

  btnDummy(){
    if( !this.cy ) return;
    // this.cy.pan({ x: 0, y: 0 });
    // this.cy.zoom({ level: 1.0, renderedPosition: { x: 0, y: 0 } });

    if( this.isDirty() ){
      this.cyUnhighlight();
    } else {
      this.actionEmitter.emit(<IEvent>{
        type: 'property-hide',
        data: this.cyPrevEvent.type !== 'idle'
      });

      this.cy.stop();       // previous animation
      this.cy.animation({
        fit: {
          eles: this.cy.elements(':visible'),
          padding: layoutPadding
        },
        duration: aniDur,
        easing: easing
      }).play();
    }
  }

  /////////////////////////////////////////////////

  undoRedoEvent(cmd:string){
    if( cmd == 'redo' ){
      this.ur.redo();
    }
    else{
      this.ur.undo();
    }
  }

  changeColor(event:IEvent){
    let color = event.data.color;
    let e = this.cy.getElementById((<IElement>event.data.target).data.id);
    let label:ILabel = _.find( (e.isNode() ? this.g.labels.nodes : this.g.labels.edges), {'name': e.data('label')} );

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('changeColor', color, e, label);

    if( e.isNode() ){
      if( label ) label.color = color;
      let elements = this.cy.nodes().filter(x=>x.data('label')==label.name);
      elements.forEach(x=>{
        x.scratch('_color', color);
        this.setStyleNode(x);
      });
      elements.connectedEdges().forEach(x=>{
        if( x.source().data('label') == label.name ) x.scratch('_color')[0] = color;
        if( x.target().data('label') == label.name ) x.scratch('_color')[1] = color;
        this.setStyleEdge(x);
      })
    }
  }

  changeIcon(event:IEvent){
    let e = this.cy.getElementById((<IElement>event.data.target).data.id);
    let label:ILabel = _.find( (e.isNode() ? this.g.labels.nodes : this.g.labels.edges), {'name': e.data('label')} );

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('changeIcon', event.data.icon, e, label);

    if( e.isNode() ){
      if( label ) label['icon'] = event.data.icon;
      let elements = this.cy.nodes().filter(x=>x.data('label')==label.name);
      elements.forEach(x=>{
        x.scratch('_icon', event.data.icon);
        this.setStyleNode(x);
      });
    }
  }

  /////////////////////////////////////////////////
  // graph contraction
  /////////////////////////////////////////////////

  gcEvent(event:IEvent){
    if( event.data == 'contraction' ){
      if( this.gc ) this.gc.contraction();
    }
    else if( event.data == 'expansion' ){
      if( this.gc ) this.gc.expansion();
    }
    else if( event.data == 'config' ){
      this.gcModalOpen();
    }
  }

  isGcUnit(item:any): boolean {
    return item.isNode() && (item.hasClass('gcunit') || item.hasClass('seed'));    // cannot access this.gcOption
  }

  gcFocusOpen(unit:any){   // node with gcunit class
    if( !this.cy ) return;

    // for DEBUG
    if( localStorage.getItem('debug')=='true' ) console.log('gcFocusOpen', unit.id(), unit.scratch());

    const modalRef:NgbModalRef = this.modalService.open(GcFocusComponent, { ariaLabelledBy: 'modal-basic-title' }); // , size: 'xl'
    modalRef.componentInstance.root = unit;
    modalRef.componentInstance.cy = this.cy;

    modalRef.result.then(
      (result)=>{ },
      (reason)=>{ }
    );
  }

  gcClear(){
    if( this.gc ){
      this.gc.clear();
      this.gc = undefined;
    }
  }

  gcModalOpen() {
    if( !this.cy ) return;

    const modalRef:NgbModalRef = this.modalService.open(GcModalComponent, { ariaLabelledBy: 'modal-basic-title' });
    modalRef.componentInstance.labels = this.g.labels.nodes.filter(x=>x.size > 0);
    modalRef.componentInstance.option = this.gcOption;
    modalRef.componentInstance.cy = this.cy;

    modalRef.result.then(
      (result)=>{
        this.gcOption = result.method == 'stop' ? undefined : result;
        this.readyEmitter.emit(<IEvent>{ type: 'gcmode-changed', data: result.method == 'stop' ? false : true });

        this.gcClear();
        if( this.gcOption )
          this.gc = new GcRunService(this.cy, result);
        //  this.gc = new GraphContraction(this.cy, result);
        // else stop;
      },
      (reason)=>{});
  }

}
