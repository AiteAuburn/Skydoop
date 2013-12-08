package ProbSkyline.DataStructures;

import java.io.Serializable;
import java.io.ObjectOutputStream;

public class instance implements Serializable{

	private static final long serialVersionUID = 42L;

	public int objectID;
	public int instanceID;
	public double prob;
	public int dimension;
	public point a_point;
	public boolean IfSkyline = true;
	public double instSkyProb = 1.0;


	public instance(int instanceID, int objectID, double prob, int dimension){
	
		this.instanceID = instanceID;
		this.objectID = objectID;
		this.prob = prob;
		this.dimension = dimension;
		this.a_point = new point(this.dimension);
	}

	public instance (int instanceID){
		this.instanceID = instanceID;	
	}

	public void setPoint(double[] coord){
		int length = coord.length;
		assert (length == a_point.dimCount): " the dimension doesn't match.";
		a_point.setPoint(coord);	
	}

	public void setInstanceID(int a_id){
		
		instanceID = a_id;
	}

	public boolean DominateAnother(instance a_inst){

		return this.a_point.DominateAnother(a_inst.a_point);
	}
	public void setObjectID(int a_id){
		
		objectID = a_id;
	}
	
	public void setProb(double a_prob){
		
		prob = a_prob;
	}
	
	public instance(point a_point){
		this.a_point = a_point;	
	}

	public double sum(point a_point){
		double ret = 0;
		for(int i=0; i<a_point.__coordinates.length; i++)
			ret+= a_point.__coordinates[i];
			
		return ret;	
	}

	public int partition(double[] split, int splitNum){
	
		double wholeWeights = sum(a_point);	
		for(int i= 0; i<splitNum; i++){
			if( (this.a_point.__coordinates[0]/wholeWeights)  <=split[i] )  { return i; }
			else continue;
		}
		return splitNum-1;
	}

	@Override
	public boolean equals(Object other){
		if(other == null) return false;
	
		int otherInstID = ((instance) other).instanceID;	
		
		if(otherInstID == this.instanceID) return true;
		else return false;
	}

	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append(Integer.toString(objectID)).append(" ");
		builder.append(Integer.toString(instanceID)).append(" ");
		builder.append(a_point.toString());
		builder.append(Double.toString(prob));

		return builder.toString();
	}

    public static class point implements Comparable<point>,Serializable {

		private static final long serialVersionUID = 43L;

		public double[] __coordinates;
		public int dimCount;

		public point(){
			__coordinates = new double[2];
			dimCount = 2;
		}

		public point(int dimCount){
			this.dimCount = dimCount;
			this.__coordinates = new double[this.dimCount];
		}

		public void setPoint(double[] coord){
		
			for(int i =0; i <dimCount; i++){
				__coordinates[i] = coord[i];
			}
		}
		
		public void setOneValue(double one_value){
		
			for(int i =0; i <dimCount; i++){
				__coordinates[i] = one_value;
			}
		}

		public void setOneValue(instance.point onePoint, double ratio){
		
			for(int i =0; i <dimCount; i++){
				__coordinates[i] = onePoint.__coordinates[i] * ratio;
			}
		}


		public void setValue(point a_center, int curr_dim, double a_side){
			this.__coordinates[curr_dim] = a_center.__coordinates[curr_dim] + a_side;
		
		}

		public boolean DominateAnother(point a_point){
			
			for(int i=0; i<dimCount; i++){
				if(__coordinates[i] <= a_point.__coordinates[i] ) continue;
				else return false; 	
			}	
			return true;	
		}

		public double sum(){

			double ret = 0;
			for(int i=0; i<__coordinates.length; i++)
				ret+= __coordinates[i];

			return ret;	

		}
		public int partition(double[] split, int splitNum){
	
			double wholeWeights = this.sum();	
			for(int i= 0; i<splitNum; i++){
				if( (__coordinates[0]/wholeWeights) <=split[i] )  { return i; }
				else continue;
			}
			return splitNum-1;
		}

        //public static int compareTo(int depth, int k, XYZPoint o1, XYZPoint o2) {
            //int axis = depth % k;
            //if (axis==X_AXIS) return X_COMPARATOR.compare(o1, o2);
            //return Y_COMPARATOR.compare(o1, o2);
        //}

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object obj) {
            if (obj==null) return false;
            if (!(obj instanceof point)) return false;

			point PT = (point) obj;
            if (this.compareTo(PT)==0) return true;
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(point o) {

			if( this.DominateAnother(o) )
				return 1;
			if( o.DominateAnother(this) )
				return -1;
			return 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
			for( int i=0; i<this.dimCount;i++){
                builder.append(Double.toString(__coordinates[i])).append(" ");
			}
            return builder.toString();
        }
    
	}
}
