import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { from, throwError, of } from 'rxjs';
import { map, share, tap, catchError, concatAll, timeout } from 'rxjs/operators';
import * as _ from 'lodash';

import { IElement } from '../models/agens-graph-types';
import { DEV_MODE } from '../app.config';

const TIMEOUT_LIMIT:number = 9999;

@Injectable({
  providedIn: 'root'
})
export class ApApiService {

  apiUrl = `${window.location.protocol}//${window.location.host}`;

  constructor(private _http: HttpClient) {
    // for DEBUG
    if( DEV_MODE ) this.apiUrl = 'http://192.168.0.30:38080';
  }

  // meta query
  // http://27.117.163.21:15632/api/admin/config
  public loadConfig() {
    let uri = this.apiUrl+'/api/admin/config';
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<any>( uri, { headers : headers });
  }

  // meta query
  // http://27.117.163.21:15632/api/admin/graphs
  public findDatasources() {
    let uri = this.apiUrl+'/api/admin/graphs';
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<any>( uri, { headers : headers });
  }

  // meta query
  // http://27.117.163.21:15632/api/admin/graphs/search/{query}
  public searchDatasources(query:string) {
    let uri = this.apiUrl+'/api/admin/graphs/search/'+query;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<any>( uri, { headers : headers });
  }

  // meta query
  // http://27.117.163.21:15632/api/admin/labels/modern
  public findLabelsByDatasource(datasource:string) {
    let uri = this.apiUrl+'/api/admin/labels/'+datasource;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<any>( uri, { headers : headers });
  }

  // gremlin query
  // http://27.117.163.21:15632/api/graph/gremlin?q=modern_g.V()
  public gremlinQuery(query:string) {
    let uri = this.apiUrl+'/api/graph/gremlin?q='+query;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<IElement[]>( uri, { headers : headers })
      .pipe(
        timeout(TIMEOUT_LIMIT),
        catchError(err=> of([]))
      );
  }

  // cypher query
  // http://27.117.163.21:15632/api/graph/gremlin?q=modern_g.V()
  public cypherQuery(datasource:string, query:string) {
    let uri = this.apiUrl+'/api/graph/cypher?ds='+datasource+'&q='+query;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<any[]>( uri, { headers : headers })
      .pipe(
        timeout(TIMEOUT_LIMIT),
        catchError(err=> of([]))
      );
  }

  // http://27.117.163.21:15632/api/search/modern/v
  public findAllByDatasource(datasource:string, index:string) { // index={v,e}
    let uri = this.apiUrl+'/api/search/'+datasource+'/'+index;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<IElement[]>( uri, { headers : headers } )
      .pipe(
        timeout(TIMEOUT_LIMIT),
        catchError(err=> of([]))
      );
  }

  // http://27.117.163.21:15632/api/search/v/modern_5
  public findById(index:string, id: string) {
    return this._http.get<IElement>(this.apiUrl+'/api/search/'+index+'/'+id);
  }

  // http://27.117.163.21:15632/api/search/modern/v/ids
  public findByIds(dataSource:string, index:string, ids: string[]) {
    let uri = this.apiUrl+'/api/search/'+dataSource+'/'+index+'/ids';
    let body = {q: ids};
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.post<IElement[]>( uri, body, { headers: headers })
      .pipe(
        timeout(TIMEOUT_LIMIT),
        catchError(err=> of([]))
      );
  }

  // post 로 바꾸어서
  public findEdgesOfVertices(dataSource: string, ids: string[]) {
    let uri =this.apiUrl+'/api/search/'+ dataSource +'/e/connected';
    let body = {q: ids};
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.post<IElement[]>( uri, body, { headers :headers })
      .pipe(
        timeout(TIMEOUT_LIMIT),
        catchError(err=> of([]))
      );
  }

  // http://27.117.163.21:15632/api/search/modern/v/value?q=ja
  public findByValueWithPartialMatch(datasource:string, index:string, value:string) {
    let uri =this.apiUrl+'/api/search/'+datasource+'/'+index+'/value?q='+value;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    return this._http.get<IElement[]>( uri, { headers: headers})
      .pipe(
        timeout(TIMEOUT_LIMIT),
        catchError(err=> of([]))
      );
  }

  // http://27.117.163.21:15632/api/search/modern/v/neighbors?q=modern_2
  public listVertexNeighbors(datasource:string, vid: string) {
    let uri = this.apiUrl+'/api/search/'+datasource+"/v/neighbors?q="+vid;
    let headers = new HttpHeaders({'Content-Type': 'application/json'});
    // response = { incomers: {}, outgoers: {} }
    return this._http.get<any>( uri, { headers: headers});
  }

}
