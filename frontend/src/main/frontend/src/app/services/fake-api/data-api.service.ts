import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class DataApiService {

  SERVER_URL: string = "http://localhost:8083/api/";
  constructor(private httpClient: HttpClient) { }
/*
  public getPolicies(){
       return this.httpClient.get(this.SERVER_URL + 'policies');
  }
  public getPolicy(policyId){
       return this.httpClient.get(`${this.SERVER_URL + 'policies'}/${policyId}`);
  }
  public createPolicy(policy: {id: number, amount: number, clientId: number, userId: number, description: string}){
      return this.httpClient.post(`${this.SERVER_URL + 'policies'}`, policy)
  }
  public deletePolicy(policyId){
      return this.httpClient.delete(`${this.SERVER_URL + 'policies'}/${policyId}`)
  }
  public updatePolicy(policy: {id: number, amount: number, clientId: number, userId: number, description: string}){
      return this.httpClient.put(`${this.SERVER_URL + 'policies'}/${policy.id}`, policy)
  }
*/
  //////////////////////////////////////////////////

  public getGraph(){
    return this.httpClient.get(this.SERVER_URL + 'graph');
  }
  public getGraphNodes(){
    return this.httpClient.get(`${this.SERVER_URL + 'graph'}/nodes`);
  }
  public getGraphEdges(){
    return this.httpClient.get(`${this.SERVER_URL + 'graph'}/links`);
  }

}
