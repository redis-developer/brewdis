import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { AgmCoreModule } from '@agm/core';
import { FlexLayoutModule } from '@angular/flex-layout';
import { HashLocationStrategy, LocationStrategy } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { ProductSearchComponent } from './product-search/product-search.component';

import { ReactiveFormsModule, FormsModule } from '@angular/forms';

import { MatButtonModule, MatIconModule, MatCardModule, MatInputModule, MatAutocompleteModule, MatListModule, MatGridListModule, MatToolbarModule, MatSelectModule, MatTableModule, MatSortModule } from '@angular/material';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { InventoryComponent } from './inventory/inventory.component';
import { AvailabilityComponent } from './availability/availability.component';
import { MaterialModule } from './material.module';

@NgModule({
  declarations: [
    AppComponent,
    ProductSearchComponent,
    InventoryComponent,
    AvailabilityComponent
  ],
  imports: [
    BrowserModule,
    AgmCoreModule.forRoot({
      apiKey: 'AIzaSyCn5UCrMs-Lh6R_kBDkGxnw9GzTME273cQ'
    }),
    BrowserAnimationsModule,
    MaterialModule,
    AppRoutingModule,
    HttpClientModule,
    ReactiveFormsModule,
    FormsModule,
    FlexLayoutModule,
    MatButtonModule,
    MatIconModule,
    MatCardModule,
    MatInputModule,
    MatAutocompleteModule,
    MatListModule,
    MatGridListModule,
    MatToolbarModule,
    MatSelectModule,
    MatTableModule,
    MatSortModule
  ],
  providers: [{provide: LocationStrategy, useClass: HashLocationStrategy}],
  bootstrap: [AppComponent]
})
export class AppModule { }
