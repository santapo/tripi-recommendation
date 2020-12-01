# HOTEL METASEARCH
This is backend of a booking app like Trivago...
It contains:
- Ranking model
- Backend API
## Backend API
- Lowest Price
- Search Query based on province, district, street
- Trending Locations
## Ranking System
- Review Score: Quality, Quantity, Old
- Price Score: Sigmoid
- Service Score: Sigmoid
- Click Through Score (search, click, book): XGBoost (*haven't achived requirements yet*)
- Final Socer: Weighted Sum above score
## Todo:
- Item based recommendation
- More ranking engine
- Improve Click Through Model 
