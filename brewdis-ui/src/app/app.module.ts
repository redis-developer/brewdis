import { AgmCoreModule } from '@agm/core';
import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FlexLayoutModule } from '@angular/flex-layout';
import { HashLocationStrategy, LocationStrategy } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { ReactiveFormsModule, FormsModule } from '@angular/forms';
import { MatDialogModule } from '@angular/material/dialog';
import { MatPaginatorModule } from '@angular/material/paginator';

import {
  MatButtonModule, MatIconModule, MatCardModule,
  MatInputModule, MatAutocompleteModule, MatListModule,
  MatGridListModule, MatToolbarModule, MatSelectModule,
  MatTableModule, MatSortModule, MatButtonToggleModule, MatExpansionModule
} from '@angular/material';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MaterialModule } from './material.module';
import { InventoryComponent } from './inventory/inventory.component';
import { AvailabilityComponent } from './availability/availability.component';
import { CatalogComponent } from './catalog/catalog.component';

@NgModule({
  declarations: [
    AppComponent,
    CatalogComponent,
    InventoryComponent,
    AvailabilityComponent
  ],
  imports: [
    AgmCoreModule.forRoot({
      apiKey: 'AIzaSyCn5UCrMs-Lh6R_kBDkGxnw9GzTME273cQ'
    }),  
    BrowserModule,
    BrowserAnimationsModule,
    MatTooltipModule,
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
    MatSortModule,
    MatButtonToggleModule,
    MatDialogModule,
    MatPaginatorModule,
    MatExpansionModule
  ],
  providers: [{ provide: LocationStrategy, useClass: HashLocationStrategy }],
  bootstrap: [AppComponent]
})
export class AppModule { }
