package ProbSkyline.Visual;

import java.awt.Dimension;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.List;

import javax.swing.*;

import java.awt.geom.Rectangle2D;
import java.awt.geom.Line2D;
import java.awt.geom.Ellipse2D;
import java.awt.Color;

import ProbSkyline.DataStructures.instance;

public class InstVisualization extends JFrame {
	public InstVisualization() {
		super("Game Frame");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Points pts= new Points();
		getContentPane().add(pts);
		for (int i = 0; i < 15; i++) {
			pts.addPoint(i * 10.0, i * 10.0);
		}

		pack();
		setLocationRelativeTo(null);
		setVisible(true);
	}

	public InstVisualization(List<instance> instList) {
		super("Inst Visual");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Points pts= new Points();
		getContentPane().add(pts);
		for (int i = 0; i < instList.size(); i++) {
			instance curr = instList.get(i);
			pts.addPoint(curr.a_point.__coordinates[0]  * 600.0, 600 - curr.a_point.__coordinates[1] * 600.0);
		}

		pack();
		setLocationRelativeTo(null);
		setVisible(true);
	}

	public InstVisualization(List<instance.point> instList, boolean point) {
		super("Inst Visual");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Points pts= new Points();
		getContentPane().add(pts);
		for (int i = 0; i < instList.size(); i++) {
			instance.point curr = instList.get(i);
			pts.addPoint(curr.__coordinates[0]  * 600.0, 600 - curr.__coordinates[1] * 600.0);
		}

		pack();
		setLocationRelativeTo(null);
		setVisible(true);
	}


	public static void main(String[] args) {
		new InstVisualization();
	}

}

class Points extends JPanel {
	private static final int PREF_W = 800;
	private static final int PREF_H = PREF_W;
	private List<Ellipse2D.Double> ellipses = new ArrayList<Ellipse2D.Double>();

	public void addPoint(double x, double y ) {
		Ellipse2D.Double el = new Ellipse2D.Double(x,y,10.0,6.0  );
		ellipses.add(el);
	}

	@Override
		public Dimension getPreferredSize() {
			return new Dimension(PREF_W, PREF_H);
		}

	@Override
		protected void paintComponent(Graphics g) {
			super.paintComponent(g);
			Graphics2D g2 = (Graphics2D) g;
			g2.setPaint(Color.blue);
			for (Ellipse2D el : ellipses) {
				g2.draw(el);
			}
			g.setColor(Color.black);
			//g.setFont(new Font("SansSerif", Font.BOLD, 8));
			g.drawString("X Axis", 600, 600);
			g.drawString("y Axis", 9, 9);
			g2.draw( new Line2D.Double(0,600,0,0));
			g2.draw( new Line2D.Double(0,600,600,600));
		}
}
