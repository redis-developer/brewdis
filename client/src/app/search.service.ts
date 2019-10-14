import { Injectable } from '@angular/core';
import { HttpClient, HttpParams, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class SearchService {

  API_URL = '/api/';

  constructor(private http: HttpClient) { }

  styles(categoryId: string): Observable<any> {
    let params = new HttpParams();
    if (categoryId != null) {
      params = params.set('categoryId', categoryId);
    }
    return this.http.get(this.API_URL + 'styles', { params });
  }

  categories(): Observable<any> {
    let params = new HttpParams();
    return this.http.get(this.API_URL + 'categories', { params });
  }

  productSearch(longitude: any, latitude: any, categoryId: string, styleId: string, query: string): Observable<any> {
    let params = new HttpParams();
    params = params.set('longitude', longitude);
    params = params.set('latitude', latitude);
    if (categoryId != null) {
      params = params.set('categoryId', categoryId);
    }
    if (styleId != null) {
      params = params.set('styleId', styleId);
    }
    if (query !== null) {
      params = params.set('query', query);
    }
    return this.http.get(this.API_URL + 'search', { params });
  }

  availability(sku: string, longitude: any, latitude: any) {
    let params = new HttpParams();
    params = params.set('sku', sku);
    params = params.set('longitude', longitude);
    params = params.set('latitude', latitude);
    return this.http.get(this.API_URL + 'availability', { params });
  }

  inventory(store: string) {
    let params = new HttpParams();
    if (store != null) {
      params = params.set('store', store);
    }
    return this.http.get(this.API_URL + 'inventory', { params });
  }

}
