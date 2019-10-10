import { Injectable } from '@angular/core';
import { HttpClient, HttpParams, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class SearchService {

  API_URL = '/api/';

  constructor(private http: HttpClient) { }

  productStyles(prefix: string): Observable<any> {
    let params = new HttpParams();
    if (prefix != null) {
      params = params.set('prefix', prefix);
    }
    return this.http.get(this.API_URL + 'products/styles', { params });
  }
  
  productCategories(): Observable<any> {
    let params = new HttpParams();
    return this.http.get(this.API_URL + 'products/categories', { params });
  }

  productSearch(category: string, style: string, query: string): Observable<any> {
    let params = new HttpParams();
    if (category != null) {
    	params = params.set('category', category);
    }
    if (style != null) {
    	params = params.set('style', style);
    }
    if (query!==null) {
      params = params.set('query', query);
    }
    return this.http.get(this.API_URL + 'products/search', { params });
  }

  likeAlbum(album: any) {
    const options = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json'
      })
    };
    this.http.post(this.API_URL + 'likes/album', album, options).subscribe(
      (val) => {
      },
      response => {
          console.log('POST call in error', response);
      },
      () => {
      });
  }
  
}
