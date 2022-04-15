import { TestBed } from '@angular/core/testing';

import { ApApiService } from './ap-api.service';
import { HttpClientModule } from '@angular/common/http';

describe('ApApiService', () => {
  beforeEach(() => TestBed.configureTestingModule({
    imports: [ HttpClientModule ],
  }));

  it('should be created', () => {
    const service: ApApiService = TestBed.get(ApApiService);
    expect(service).toBeTruthy();
  });
});
