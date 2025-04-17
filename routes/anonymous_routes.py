from flask import Blueprint, jsonify
from recommend.trending_popular import get_latest_metadata, get_diversified_trending_popular

anonymous_bp = Blueprint('anonymous_bp', __name__)

@anonymous_bp.route('/recommend-anonymous', methods=['GET'])
def recommend_for_anonymous():
    try:
        df_metadata = get_latest_metadata()
        df = get_diversified_trending_popular(df_metadata, top_k_each=5)
        return jsonify({'user_id': 'anonymous', 'recommendations': df[['product_id', 'score', 'source']].to_dict(orient='records')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
