import { Injectable } from '@angular/core';
import { HttpClient, HttpParams, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../environments/environment';

export interface Query {
  query: string;
  sortByField: string;
  sortByDirection: string;
  pageIndex: number;
  pageSize: number;
}

@Injectable({
  providedIn: 'root'
})
export class SearchService {

  API_URL = '/api/';

  constructor(private http: HttpClient) { }

  styles(category: string): Observable<any> {
    let params = new HttpParams();
    if (category != null) {
      params = params.set('category', category);
    }
    return this.http.get(this.API_URL + 'styles', { params });
  }

  categories(): Observable<any> {
    return this.http.get(this.API_URL + 'categories');
  }

  foods(): Observable<any> {
    return this.http.get(this.API_URL + 'foods');
  }

  products(query: Query, lng: any, lat: any): Observable<any> {
    let params = new HttpParams();
    params = params.set('longitude', lng);
    params = params.set('latitude', lat);
    return this.http.post(this.API_URL + 'products', query, { params });
  }

  availability(sku: string, lng: any, lat: any) {
    let params = new HttpParams();
    if (sku != null) {
      params = params.set('sku', sku);
    }
    params = params.set('longitude', lng);
    params = params.set('latitude', lat);
    return this.http.get(this.API_URL + 'availability', { params });
  }

  inventory(store: string) {
    let params = new HttpParams();
    if (store != null) {
      params = params.set('store', store);
    }
    return this.http.get(this.API_URL + 'inventory', { params });
  }

  suggestBreweries(prefix: string): Observable<any> {
    let params = new HttpParams();
    if (prefix != null) {
      params = params.set('prefix', prefix);
    }
    return this.http.get(this.API_URL + 'breweries', { params });
  }

  suggestFoods(prefix: string): Observable<any> {
    let params = new HttpParams();
    if (prefix != null) {
      params = params.set('prefix', prefix);
    }
    return this.http.get(this.API_URL + 'foods', { params });
  }

}
